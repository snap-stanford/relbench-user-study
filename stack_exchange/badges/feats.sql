create or replace table badges_{{ set }}_feats as -- noqa

with labels as (
    select * from badges_{{ set }} -- noqa
),

badge_freqs as (
    select
        Name,
        count(*) / (sum(count(*)) over ()) as badge_incidence
    from badges
    group by Name
),

badge_feats_deagged as (
    select
        labels.UserId as user_id,
        labels.timestamp,
        badges.Id,
        log(1 / badge_freqs.badge_incidence) as rarity,
        date_diff('week', badges.Date, labels.timestamp) as badge_age_weeks,
        date_diff(
            'week',
            lag(badges.Date, 1) over (
                partition by badges.UserId order by badges.Date asc
            ),
            badges.Date
        ) as weeks_since_prev_badge,
        -- exponential decay results in ~6% of original weight after 1 year
        (rarity * 0.95**badge_age_weeks) as smoothed_weight
    from labels
    left join badges
        on
            labels.UserId = badges.UserId
            and badges.Date between (labels.timestamp - interval 2 year) and labels.timestamp
    left join badge_freqs
        on badges.Name = badge_freqs.Name
),

badge_feats as (
    select
        user_id,
        timestamp,
        coalesce(count(distinct Id), 0) as num_badges,
        coalesce(sum(rarity), 0) as badge_score,
        coalesce(max(rarity), 0) as max_rarity,
        coalesce(avg(rarity), 0) as avg_rarity,
        first(badge_age_weeks order by rarity desc) as rarest_badge_age_weeks,
        first(rarity order by badge_age_weeks asc) as last_badge_rarity,
        min(badge_age_weeks) as last_badge_weeks_ago,
        avg(badge_age_weeks) as avg_badge_age_weeks,
        avg(weeks_since_prev_badge) as avg_weeks_bw_badges,
        coalesce(sum(smoothed_weight), 0) as badge_momentum
    from badge_feats_deagged
    group by user_id, timestamp
),

user_feats as (
    select
        labels.UserId as user_id,
        labels.timestamp,
        date_diff('month', users.CreationDate, labels.timestamp) as months_since_account_creation
    from labels
    left join users
        on labels.UserId = users.Id
),

comment_aggs_by_user as (
    select
        labels.UserId as user_id,
        labels.timestamp,
        min(date_diff('week', comments.CreationDate, labels.timestamp)) as weeks_since_last_comment,
        count(distinct comments.Id) as num_comments,
        count(distinct comments.PostId) as num_posts_commented,
        avg(coalesce(len(string_split(comments.Text, ' ')), 0)) as avg_comment_length
    from labels
    left join comments
        on
            labels.UserId = comments.UserId
            and labels.timestamp > comments.CreationDate
    group by all
),

-- A row per (time-censored) post per user, timestamp tuple.
post_labels as (
    select
        labels.UserId as user_id,
        labels.timestamp,
        posts.Id as post_id
    from labels
    left join posts
        on
            labels.UserId = posts.OwnerUserId
            and labels.timestamp > posts.CreationDate
),

vote_aggs as (
    select
        post_labels.user_id,
        post_labels.timestamp,
        post_labels.post_id,
        count(case when votes.VoteTypeId in (1, 2, 5, 8, 16) then 1 end) as num_positive_votes,
        count(case when votes.VoteTypeId in (3, 4, 6, 10, 12) then 1 end) as num_negative_votes
    from post_labels
    left join votes
        on
            post_labels.post_id = votes.PostId
            and post_labels.timestamp > votes.CreationDate
    group by all
),

comment_aggs_by_post as (
    select
        post_labels.post_id,
        post_labels.user_id,
        post_labels.timestamp,
        count(distinct comments.Id) as num_comments,
        avg(coalesce(badge_feats.badge_score, 0)) as avg_badge_score
    from post_labels
    left join comments
        on
            post_labels.post_id = comments.PostId
            and post_labels.timestamp > comments.CreationDate
    left join badge_feats
        on
            comments.UserId = badge_feats.user_id
            and post_labels.timestamp = badge_feats.timestamp
    group by all
),

post_feats_deagged as (
    select
        post_labels.post_id,
        post_labels.user_id,
        post_labels.timestamp,
        posts.CreationDate as creation_date,
        row_number() over (
            partition by post_labels.user_id, post_labels.timestamp, posts.PostTypeId
            order by posts.CreationDate desc
        ) as post_rank,
        posts.PostTypeId as post_type,
        coalesce(len(string_split(trim(posts.Tags, '<>'), '><')), 0) as num_tags,
        len(string_split(posts.Body, ' ')) as body_length,
        date_diff(
            'days',
            lag(posts.CreationDate, 1) over (
                partition by posts.OwnerUserId order by posts.CreationDate asc
            ),
            posts.CreationDate
        ) as days_since_last_post,
        coalesce(vote_aggs.num_positive_votes, 0) as num_positive_votes,
        coalesce(vote_aggs.num_negative_votes, 0) as num_negative_votes,
        coalesce(comment_aggs_by_post.num_comments, 0) as num_comments,
        comment_aggs_by_post.avg_badge_score as avg_commenter_badge_score
    from post_labels
    left join posts
        on post_labels.post_id = posts.Id
    left join vote_aggs
        on
            post_labels.post_id = vote_aggs.post_id
            and post_labels.user_id = vote_aggs.user_id
            and post_labels.timestamp = vote_aggs.timestamp
    left join comment_aggs_by_post
        on
            post_labels.post_id = comment_aggs_by_post.post_id
            and post_labels.user_id = comment_aggs_by_post.user_id
            and post_labels.timestamp = comment_aggs_by_post.timestamp
),

last_question_feats as (
    select
        *,
        date_diff('week', creation_date, timestamp) as last_q_weeks_ago
    from post_feats_deagged
    where
        post_type = 1
        and post_rank = 1
),

last_answer_feats as (
    select
        *,
        date_diff('week', creation_date, timestamp) as last_a_weeks_ago
    from post_feats_deagged
    where
        post_type = 2
        and post_rank = 1
),

question_feats_last_yr as (
    select
        user_id,
        timestamp,
        count(*) as num_questions_last_yr,
        avg(days_since_last_post) as avg_days_since_last_post_q,
        avg(num_tags) as avg_num_tags,
        avg(body_length) as avg_body_length,
        avg(num_positive_votes) as avg_num_positive_votes,
        avg(num_negative_votes) as avg_num_negative_votes,
        avg(num_comments) as avg_num_comments,
        avg(avg_commenter_badge_score) as avg_commenter_badge_score
    from post_feats_deagged
    where
        date_diff('month', creation_date, timestamp) <= 12
        and post_type = 1
    group by all
),

answer_feats_last_yr as (
    select
        user_id,
        timestamp,
        count(*) as num_answers_last_yr,
        avg(days_since_last_post) as avg_days_since_last_post_a,
        avg(body_length) as avg_body_length,
        avg(num_positive_votes) as avg_num_positive_votes,
        avg(num_negative_votes) as avg_num_negative_votes,
        avg(num_comments) as avg_num_comments,
        avg(avg_commenter_badge_score) as avg_commenter_badge_score
    from post_feats_deagged
    where
        date_diff('month', creation_date, timestamp) <= 12
        and post_type = 2
    group by all
)

-- Final feature set
select
    -- labels
    labels.UserId as user_id,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.WillGetBadge,
    {% endif %}
    -- user-level features
    user_feats.months_since_account_creation,
    badge_feats.num_badges,
    badge_feats.badge_score,
    badge_feats.max_rarity,
    badge_feats.avg_rarity,
    badge_feats.rarest_badge_age_weeks,
    badge_feats.last_badge_rarity,
    badge_feats.last_badge_weeks_ago,
    badge_feats.avg_badge_age_weeks,
    badge_feats.avg_weeks_bw_badges,
    badge_feats.badge_momentum,
    comment_aggs_by_user.weeks_since_last_comment,
    comment_aggs_by_user.num_comments,
    comment_aggs_by_user.num_posts_commented,
    comment_aggs_by_user.avg_comment_length,
    -- question features
    last_question_feats.last_q_weeks_ago,
    last_question_feats.num_tags as last_q_num_tags,
    last_question_feats.body_length as last_q_body_length,
    last_question_feats.num_positive_votes as last_q_num_positive_votes,
    last_question_feats.num_negative_votes as last_q_num_negative_votes,
    last_question_feats.num_comments as last_q_num_comments,
    last_question_feats.avg_commenter_badge_score as last_q_avg_commenter_badge_score,
    question_feats_last_yr.num_questions_last_yr,
    question_feats_last_yr.avg_days_since_last_post_q,
    question_feats_last_yr.avg_num_tags,
    question_feats_last_yr.avg_body_length as avg_body_length_q,
    question_feats_last_yr.avg_num_positive_votes as avg_num_positive_votes_q,
    question_feats_last_yr.avg_num_negative_votes as avg_num_negative_votes_q,
    question_feats_last_yr.avg_num_comments as avg_num_comments_q,
    question_feats_last_yr.avg_commenter_badge_score as avg_commenter_badge_score_q,
    -- answer features
    last_answer_feats.last_a_weeks_ago,
    last_answer_feats.body_length as last_a_body_length,
    last_answer_feats.num_positive_votes as last_a_num_positive_votes,
    last_answer_feats.num_negative_votes as last_a_num_negative_votes,
    last_answer_feats.num_comments as last_a_num_comments,
    last_answer_feats.avg_commenter_badge_score as last_a_avg_commenter_badge_score,
    answer_feats_last_yr.num_answers_last_yr,
    answer_feats_last_yr.avg_days_since_last_post_a,
    answer_feats_last_yr.avg_body_length as avg_body_length_a,
    answer_feats_last_yr.avg_num_positive_votes as avg_num_positive_votes_a,
    answer_feats_last_yr.avg_num_negative_votes as avg_num_negative_votes_a,
    answer_feats_last_yr.avg_num_comments as avg_num_comments_a,
    answer_feats_last_yr.avg_commenter_badge_score as avg_commenter_badge_score_a

from labels
left join user_feats
    on
        labels.UserId = user_feats.user_id
        and labels.timestamp = user_feats.timestamp
left join badge_feats
    on
        labels.UserId = badge_feats.user_id
        and labels.timestamp = badge_feats.timestamp
left join comment_aggs_by_user
    on
        labels.UserId = comment_aggs_by_user.user_id
        and labels.timestamp = comment_aggs_by_user.timestamp
left join last_question_feats
    on
        labels.UserId = last_question_feats.user_id
        and labels.timestamp = last_question_feats.timestamp
left join last_answer_feats
    on
        labels.UserId = last_answer_feats.user_id
        and labels.timestamp = last_answer_feats.timestamp
left join question_feats_last_yr
    on
        labels.UserId = question_feats_last_yr.user_id
        and labels.timestamp = question_feats_last_yr.timestamp
left join answer_feats_last_yr
    on
        labels.UserId = answer_feats_last_yr.user_id
        and labels.timestamp = answer_feats_last_yr.timestamp;
