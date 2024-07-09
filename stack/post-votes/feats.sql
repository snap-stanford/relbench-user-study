create or replace table votes_{{ set }}_feats as -- noqa

with labels as materialized (
    -- Schema:
    --     timestamp
    --     PostId
    --     popularity
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from votes_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from votes_{{ set }} -- noqa
    {% endif %}
),

post_attrs_at_creation as (
    select
        PostId,
        any_value(case when PostHistoryTypeId = 1 then Text end) as orig_title,
        any_value(case when PostHistoryTypeId = 2 then Text end) as orig_body,
        any_value(case when PostHistoryTypeId = 3 then Text end) as orig_tags
    from post_history
    where PostHistoryTypeId in (1, 2, 3)
    group by PostId
),

post_hist_feats as (
    select
        labels.PostId,
        labels.timestamp,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 10 then post_history.CreationDate end,
            labels.timestamp
        )) as closed_weeks_ago,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 11 then post_history.CreationDate end,
            labels.timestamp
        )) as reopened_weeks_ago,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 12 then post_history.CreationDate end,
            labels.timestamp
        )) as deleted_weeks_ago,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 13 then post_history.CreationDate end,
            labels.timestamp
        )) as undeleted_weeks_ago,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 14 then post_history.CreationDate end,
            labels.timestamp
        )) as locked_weeks_ago,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 15 then post_history.CreationDate end,
            labels.timestamp
        )) as unlocked_weeks_ago,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 25 then post_history.CreationDate end,
            labels.timestamp
        )) as tweeted_weeks_ago,
        min(date_diff(
            'week',
            case when post_history.PostHistoryTypeId = 50 then post_history.CreationDate end,
            labels.timestamp
        )) as bumped_weeks_ago
    from labels
    left join post_history
        on
            labels.PostId = post_history.PostId
            and labels.timestamp > post_history.CreationDate
    where post_history.PostHistoryTypeId in (10, 11, 12, 13, 14, 15, 25, 50)
    group by all
),

post_ordinals as (
    select
        Id as PostId,
        row_number() over (partition by OwnerUserId order by CreationDate asc) as post_ordinal
    from posts
),

num_votes as (
    select
        labels.PostId,
        labels.timestamp,
        count(*) as num_votes
    from labels
    left join votes
        on
            labels.PostId = votes.PostId
            and labels.timestamp > votes.CreationDate
    group by all
),

vote_stats_first_month as (
    select
        votes.PostId,
        count(*) as num_votes
    from votes
    left join posts
        on votes.PostId = posts.Id
    where
        votes.VoteTypeId = 2  -- upvotes
        and date_diff('day', posts.CreationDate, votes.CreationDate) <= 30
    group by votes.PostId
),

comment_stats_first_month as (
    select
        comments.PostId,
        count(*) as num_comments
    from comments
    left join posts
        on comments.PostId = posts.Id
    where date_diff('day', posts.CreationDate, comments.CreationDate) <= 30
    group by comments.PostId
),

user_timestamp as (
    -- aggregate to (user, timestamp) first to speed up query
    select
        labels.timestamp,
        posts.OwnerUserId as user_id
    from labels
    left join posts
        on labels.PostId = posts.Id
    group by all
),

owner_feats_by_timestamp as (
    select
        user_timestamp.timestamp,
        user_timestamp.user_id,
        avg(
            case
                when censored_posts.PostTypeId = 1
                    then coalesce(vote_stats_first_month.num_votes, 0)
            end
        ) as avg_owner_question_upvotes_first_month,
        avg(
            case
                when censored_posts.PostTypeId = 1
                    then coalesce(comment_stats_first_month.num_comments, 0)
            end
        ) as avg_owner_question_comments_first_month,
        avg(
            case
                when censored_posts.PostTypeId = 2
                    then coalesce(vote_stats_first_month.num_votes, 0)
            end
        ) as avg_owner_answer_upvotes_first_month,
        avg(
            case
                when censored_posts.PostTypeId = 2
                    then coalesce(comment_stats_first_month.num_comments, 0)
            end
        ) as avg_owner_answer_comments_first_month
    from user_timestamp
    left join posts as censored_posts
        on
            user_timestamp.user_id = censored_posts.OwnerUserId
            and date_diff('day', censored_posts.CreationDate, user_timestamp.timestamp) > 30
    left join vote_stats_first_month
        on censored_posts.Id = vote_stats_first_month.PostId
    left join comment_stats_first_month
        on censored_posts.Id = comment_stats_first_month.PostId
    group by all
),

post_feats as (
    select
        labels.PostId,
        labels.timestamp,
        posts.PostTypeId as post_type,
        date_diff('week', posts.CreationDate, labels.timestamp) as post_age_weeks,
        coalesce(len(post_attrs_at_creation.orig_title), 0) as title_length,
        coalesce(len(post_attrs_at_creation.orig_body), 0) as body_length,
        coalesce(len(string_split(trim(posts.Tags, '<>'), '><')), 0) as num_tags,
        date_diff('month', users.CreationDate, labels.timestamp) as user_age_months,
        post_ordinals.post_ordinal,
        num_votes.num_votes,
        post_hist_feats.closed_weeks_ago,
        post_hist_feats.reopened_weeks_ago,
        post_hist_feats.deleted_weeks_ago,
        post_hist_feats.undeleted_weeks_ago,
        post_hist_feats.locked_weeks_ago,
        post_hist_feats.unlocked_weeks_ago,
        post_hist_feats.tweeted_weeks_ago,
        post_hist_feats.bumped_weeks_ago,
        owner_feats_by_timestamp.avg_owner_question_upvotes_first_month,
        owner_feats_by_timestamp.avg_owner_question_comments_first_month,
        owner_feats_by_timestamp.avg_owner_answer_upvotes_first_month,
        owner_feats_by_timestamp.avg_owner_answer_comments_first_month
    from labels
    left join posts
        on labels.PostId = posts.Id
    left join post_attrs_at_creation
        on labels.PostId = post_attrs_at_creation.PostId
    left join users
        on posts.OwnerUserId = users.Id
    left join post_ordinals
        on labels.PostId = post_ordinals.PostId
    left join num_votes
        on
            labels.PostId = num_votes.PostId
            and labels.timestamp = num_votes.timestamp
    left join post_hist_feats
        on
            labels.PostId = post_hist_feats.PostId
            and labels.timestamp = post_hist_feats.timestamp
    left join owner_feats_by_timestamp
        on
            posts.OwnerUserId = owner_feats_by_timestamp.user_id
            and labels.timestamp = owner_feats_by_timestamp.timestamp
)

-- Final Feature Set
select
    labels.PostId,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.popularity,
    {% endif %}
    post_feats.post_type,
    post_feats.post_age_weeks,
    post_feats.title_length,
    post_feats.body_length,
    post_feats.num_tags,
    post_feats.user_age_months,
    post_feats.post_ordinal,
    post_feats.num_votes,
    post_feats.closed_weeks_ago,
    post_feats.reopened_weeks_ago,
    post_feats.deleted_weeks_ago,
    post_feats.undeleted_weeks_ago,
    post_feats.locked_weeks_ago,
    post_feats.unlocked_weeks_ago,
    post_feats.tweeted_weeks_ago,
    post_feats.bumped_weeks_ago,
    post_feats.avg_owner_question_upvotes_first_month,
    post_feats.avg_owner_question_comments_first_month,
    post_feats.avg_owner_answer_upvotes_first_month,
    post_feats.avg_owner_answer_comments_first_month
from labels
left join post_feats
    on
        labels.PostId = post_feats.PostId
        and labels.timestamp = post_feats.timestamp
