create or replace table user_engagement_{{ set }}_feats as -- noqa

with labels as materialized (
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from user_engagement_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from user_engagement_{{ set }} -- noqa
    {% endif %}
),

badge_freqs as (
    select
        Name,
        count(*) / (sum(count(*)) over ()) as badge_incidence
    from badges
    group by Name
),

badge_feats as (
    select
        labels.OwnerUserId,
        labels.timestamp,
        coalesce(count(distinct badges.Id), 0) as num_badges,
        coalesce(sum(log(1 / badge_freqs.badge_incidence)), 0) as badge_score
    from labels
    left join badges
        on
            labels.OwnerUserId = badges.UserId
            and labels.timestamp > badges.Date
    left join badge_freqs
        on badges.Name = badge_freqs.Name
    group by all
),

user_feats as (
    select
        labels.OwnerUserId,
        labels.timestamp,
        date_diff('month', users.CreationDate, labels.timestamp) as months_since_account_creation,
        (users.DisplayName is null) as display_name_is_null,
        (users.WebsiteUrl is null) as website_url_is_null,
        coalesce(len(string_split(users.AboutMe, ' ')), 0) as about_me_length,
        (users.Location is null) as location_is_null
    from labels
    left join users
        on labels.OwnerUserId = users.Id
),

comment_aggs_by_user as (
    select
        labels.OwnerUserId,
        labels.timestamp,
        min(date_diff('week', comments.CreationDate, labels.timestamp)) as weeks_since_last_comment,
        count(distinct comments.Id) as num_comments,
        count(distinct comments.PostId) as num_posts_commented,
        avg(coalesce(len(string_split(comments.Text, ' ')), 0)) as avg_comment_length
    from labels
    left join comments
        on
            labels.OwnerUserId = comments.UserId
            and labels.timestamp > comments.CreationDate
    group by all
),

-- A row per (time-censored) post per user, timestamp tuple.
post_labels as (
    select
        labels.OwnerUserId,
        labels.timestamp,
        posts.Id as post_id
    from labels
    left join posts
        on
            labels.OwnerUserId = posts.OwnerUserId
            and labels.timestamp > posts.CreationDate
),

vote_aggs as (
    select
        post_labels.OwnerUserId,
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
        post_labels.OwnerUserId,
        post_labels.timestamp,
        count(distinct comments.Id) as num_comments,
        avg(coalesce(len(string_split(comments.Text, ' ')), 0)) as avg_comment_length,
        count(distinct comments.UserId) as num_distinct_users,
        avg(coalesce(badge_feats.badge_score, 0)) as avg_badge_score
    from post_labels
    left join comments
        on
            post_labels.post_id = comments.PostId
            and post_labels.timestamp > comments.CreationDate
    left join badge_feats
        on
            comments.UserId = badge_feats.OwnerUserId
            and post_labels.timestamp = badge_feats.timestamp
    group by all
),

accepted_answers as (
    select AcceptedAnswerId as accepted_ans_id
    from posts
    where AcceptedAnswerId is not null
),

post_feats_deagged as (
    select
        post_labels.post_id,
        post_labels.OwnerUserId,
        post_labels.timestamp,
        posts.CreationDate as creation_date,
        row_number() over (
            partition by post_labels.OwnerUserId, post_labels.timestamp, posts.PostTypeId
            order by posts.CreationDate desc
        ) as post_rank,
        posts.PostTypeId as post_type,
        (posts.AcceptedAnswerId is not null) as has_accepted_ans,
        (accepted_answers.accepted_ans_id is not null) as is_accepted_ans,
        coalesce(len(string_split(trim(posts.Tags, '<>'), '><')), 0) as num_tags,
        len(string_split(posts.Title, ' ')) as title_length,
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
        coalesce(comment_aggs_by_post.avg_comment_length, 0) as avg_comment_length,
        coalesce(comment_aggs_by_post.num_distinct_users, 0) as num_distinct_commenters,
        comment_aggs_by_post.avg_badge_score as avg_commenter_badge_score
    from post_labels
    left join posts
        on post_labels.post_id = posts.Id
    left join accepted_answers
        on post_labels.post_id = accepted_answers.accepted_ans_id
    left join vote_aggs
        on
            post_labels.post_id = vote_aggs.post_id
            and post_labels.OwnerUserId = vote_aggs.OwnerUserId
            and post_labels.timestamp = vote_aggs.timestamp
    left join comment_aggs_by_post
        on
            post_labels.post_id = comment_aggs_by_post.post_id
            and post_labels.OwnerUserId = comment_aggs_by_post.OwnerUserId
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

question_feats_last_6mo as (
    select
        OwnerUserId,
        timestamp,
        count(*) as num_questions_last_6mo,
        avg(has_accepted_ans::int) as avg_has_accepted_ans,
        avg(days_since_last_post) as avg_days_since_last_post_q,
        avg(num_tags) as avg_num_tags,
        avg(title_length) as avg_title_length,
        avg(body_length) as avg_body_length,
        avg(num_positive_votes) as avg_num_positive_votes,
        avg(num_negative_votes) as avg_num_negative_votes,
        avg(num_comments) as avg_num_comments,
        avg(avg_comment_length) as avg_avg_comment_length,
        avg(num_distinct_commenters) as avg_num_distinct_commenters,
        avg(avg_commenter_badge_score) as avg_commenter_badge_score
    from post_feats_deagged
    where
        date_diff('month', creation_date, timestamp) <= 6
        and post_type = 1
    group by all
),

answer_feats_last_6mo as (
    select
        OwnerUserId,
        timestamp,
        count(*) as num_answers_last_6mo,
        avg(is_accepted_ans::int) as ans_acceptance_rate,
        avg(days_since_last_post) as avg_days_since_last_post_a,
        avg(body_length) as avg_body_length,
        avg(num_positive_votes) as avg_num_positive_votes,
        avg(num_negative_votes) as avg_num_negative_votes,
        avg(num_comments) as avg_num_comments,
        avg(avg_comment_length) as avg_avg_comment_length,
        avg(num_distinct_commenters) as avg_num_distinct_commenters,
        avg(avg_commenter_badge_score) as avg_commenter_badge_score
    from post_feats_deagged
    where
        date_diff('month', creation_date, timestamp) <= 6
        and post_type = 2
    group by all
)

-- Final feature set
select
    -- labels
    labels.OwnerUserId,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.contribution,
    {% endif %}
    -- user-level features
    user_feats.months_since_account_creation,
    user_feats.display_name_is_null,
    user_feats.website_url_is_null,
    user_feats.about_me_length,
    user_feats.location_is_null,
    badge_feats.num_badges,
    badge_feats.badge_score,
    comment_aggs_by_user.weeks_since_last_comment,
    comment_aggs_by_user.num_comments,
    comment_aggs_by_user.num_posts_commented,
    comment_aggs_by_user.avg_comment_length,
    -- question features
    last_question_feats.last_q_weeks_ago,
    last_question_feats.has_accepted_ans as last_q_has_accepted_ans,
    last_question_feats.num_tags as last_q_num_tags,
    last_question_feats.title_length as last_q_title_length,
    last_question_feats.body_length as last_q_body_length,
    last_question_feats.num_positive_votes as last_q_num_positive_votes,
    last_question_feats.num_negative_votes as last_q_num_negative_votes,
    last_question_feats.num_comments as last_q_num_comments,
    last_question_feats.avg_comment_length as last_q_avg_comment_length,
    last_question_feats.num_distinct_commenters as last_q_num_distinct_commenters,
    last_question_feats.avg_commenter_badge_score as last_q_avg_commenter_badge_score,
    question_feats_last_6mo.num_questions_last_6mo,
    question_feats_last_6mo.avg_has_accepted_ans,
    question_feats_last_6mo.avg_days_since_last_post_q,
    question_feats_last_6mo.avg_num_tags,
    question_feats_last_6mo.avg_title_length as avg_title_length_q,
    question_feats_last_6mo.avg_body_length as avg_body_length_q,
    question_feats_last_6mo.avg_num_positive_votes as avg_num_positive_votes_q,
    question_feats_last_6mo.avg_num_negative_votes as avg_num_negative_votes_q,
    question_feats_last_6mo.avg_num_comments as avg_num_comments_q,
    question_feats_last_6mo.avg_avg_comment_length as avg_avg_comment_length_q,
    question_feats_last_6mo.avg_num_distinct_commenters as avg_num_distinct_commenters_q,
    question_feats_last_6mo.avg_commenter_badge_score as avg_commenter_badge_score_q,
    -- answer features
    last_answer_feats.last_a_weeks_ago,
    last_answer_feats.is_accepted_ans as last_a_is_accepted_ans,
    last_answer_feats.body_length as last_a_body_length,
    last_answer_feats.num_positive_votes as last_a_num_positive_votes,
    last_answer_feats.num_negative_votes as last_a_num_negative_votes,
    last_answer_feats.num_comments as last_a_num_comments,
    last_answer_feats.avg_comment_length as last_a_avg_comment_length,
    last_answer_feats.num_distinct_commenters as last_a_num_distinct_commenters,
    last_answer_feats.avg_commenter_badge_score as last_a_avg_commenter_badge_score,
    answer_feats_last_6mo.num_answers_last_6mo,
    answer_feats_last_6mo.ans_acceptance_rate,
    answer_feats_last_6mo.avg_days_since_last_post_a,
    answer_feats_last_6mo.avg_body_length as avg_body_length_a,
    answer_feats_last_6mo.avg_num_positive_votes as avg_num_positive_votes_a,
    answer_feats_last_6mo.avg_num_negative_votes as avg_num_negative_votes_a,
    answer_feats_last_6mo.avg_num_comments as avg_num_comments_a,
    answer_feats_last_6mo.avg_avg_comment_length as avg_avg_comment_length_a,
    answer_feats_last_6mo.avg_num_distinct_commenters as avg_num_distinct_commenters_a,
    answer_feats_last_6mo.avg_commenter_badge_score as avg_commenter_badge_score_a

from labels
left join user_feats
    on
        labels.OwnerUserId = user_feats.OwnerUserId
        and labels.timestamp = user_feats.timestamp
left join badge_feats
    on
        labels.OwnerUserId = badge_feats.OwnerUserId
        and labels.timestamp = badge_feats.timestamp
left join comment_aggs_by_user
    on
        labels.OwnerUserId = comment_aggs_by_user.OwnerUserId
        and labels.timestamp = comment_aggs_by_user.timestamp
left join last_question_feats
    on
        labels.OwnerUserId = last_question_feats.OwnerUserId
        and labels.timestamp = last_question_feats.timestamp
left join last_answer_feats
    on
        labels.OwnerUserId = last_answer_feats.OwnerUserId
        and labels.timestamp = last_answer_feats.timestamp
left join question_feats_last_6mo
    on
        labels.OwnerUserId = question_feats_last_6mo.OwnerUserId
        and labels.timestamp = question_feats_last_6mo.timestamp
left join answer_feats_last_6mo
    on
        labels.OwnerUserId = answer_feats_last_6mo.OwnerUserId
        and labels.timestamp = answer_feats_last_6mo.timestamp;
