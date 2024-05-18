create or replace table product_churn_{{ set }}_feats as -- noqa

with labels as (
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from product_churn_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from product_churn_{{ set }} -- noqa
    {% endif %}
),

product_feats as (
    select
        labels.product_id,
        labels.timestamp,
        any_value(product.price) as price,
        any_value(product.category[-1]) as category,
        any_value(product.title) as title,
        max(date_diff('weeks', review.review_time, labels.timestamp)) as weeks_since_first_review,
        median(
            date_diff('weeks', review.review_time, labels.timestamp)
        ) as weeks_since_median_review,
        min(date_diff('weeks', review.review_time, labels.timestamp)) as weeks_since_latest_review,
        count(*) as num_reviews,
        sum(review.rating) as sum_ratings,
        avg(review.rating) as avg_rating,
        stddev(review.rating) as std_rating,
        min(review.rating) as min_rating,
        max(review.rating) as max_rating,
        avg(review.verified::int) as pct_verified_reviews,
        avg(length(review.review_text)) as avg_review_length,
        arg_max(review.summary, review.review_time) as last_review_summary,
        -- Using avg of avg rating as a proxy for global avg rating
        (avg_rating - avg(avg_rating) over ()) / stddev(avg_rating) over () as product_bias
    from labels
    left join product
        on labels.product_id = product.product_id
    left join review
        on
            labels.product_id = review.product_id
            and labels.timestamp > review.review_time
    group by all
),

timestamps as (
    select distinct timestamp from labels
),

customer_feats as (
    select
        timestamps.timestamp,
        review.customer_id,
        count(*) as num_reviews,
        sum(product.price) as total_spent,
        avg(product.price) as avg_price,
        avg(review.rating) as avg_rating,
        stddev(review.rating) as std_rating
    from timestamps
    left join review
        on timestamps.timestamp > review.review_time
    left join product
        on review.product_id = product.product_id
    group by all
),

reviewer_aggs as (
    select
        labels.product_id,
        labels.timestamp,
        avg(customer_feats.num_reviews) as avg_reviewer_num_reviews,
        avg(customer_feats.total_spent) as avg_reviewer_total_spent,
        avg(customer_feats.avg_price) as avg_reviewer_avg_price,
        avg(customer_feats.avg_rating) as avg_reviewer_avg_rating,
        avg(customer_feats.std_rating) as avg_reviewer_std_rating
    from labels
    left join review
        on
            labels.product_id = review.product_id
            and labels.timestamp > review.review_time
    left join customer_feats
        on
            review.customer_id = customer_feats.customer_id
            and labels.timestamp = customer_feats.timestamp
    group by all
),

last_6mo as (
    select
        labels.product_id,
        labels.timestamp,
        count(*) as num_reviews,
        avg(review.rating) as avg_rating,
        sum(review.rating) as sum_ratings,
        min(review.rating) as min_rating,
        max(review.rating) as max_rating,
        avg(length(review.review_text)) as avg_review_length,
        (avg_rating - avg(avg_rating) over ()) / stddev(avg_rating) over () as product_bias
    from labels
    left join review
        on
            labels.product_id = review.product_id
            and labels.timestamp > review.review_time
            and labels.timestamp - interval '6 months' < review.review_time
    group by all
),

prev_6mo as (
    select
        labels.product_id,
        labels.timestamp,
        count(*) as num_reviews,
        avg(review.rating) as avg_rating,
        sum(review.rating) as sum_ratings,
        min(review.rating) as min_rating,
        max(review.rating) as max_rating,
        avg(length(review.review_text)) as avg_review_length,
        (avg_rating - avg(avg_rating) over ()) / stddev(avg_rating) over () as product_bias
    from labels
    left join review
        on
            labels.product_id = review.product_id
            and labels.timestamp - interval '6 months' > review.review_time
            and labels.timestamp - interval '12 months' < review.review_time
    group by all
),

trends as (
    select
        labels.product_id,
        labels.timestamp,
        (last_6mo.num_reviews - prev_6mo.num_reviews) as num_reviews_trend,
        (last_6mo.avg_rating - prev_6mo.avg_rating) as avg_rating_trend,
        (last_6mo.sum_ratings - prev_6mo.sum_ratings) as sum_ratings_trend,
        (last_6mo.min_rating - prev_6mo.min_rating) as min_rating_trend,
        (last_6mo.max_rating - prev_6mo.max_rating) as max_rating_trend,
        (last_6mo.avg_review_length - prev_6mo.avg_review_length) as avg_review_length_trend,
        (last_6mo.product_bias - prev_6mo.product_bias) as product_bias_trend
    from labels
    left join last_6mo
        on
            labels.product_id = last_6mo.product_id
            and labels.timestamp = last_6mo.timestamp
    left join prev_6mo
        on
            labels.product_id = prev_6mo.product_id
            and labels.timestamp = prev_6mo.timestamp
)

select
    labels.product_id,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.churn,
    {% endif %}
    product_feats.price,
    product_feats.category,
    product_feats.title,
    product_feats.weeks_since_first_review,
    product_feats.weeks_since_median_review,
    product_feats.weeks_since_latest_review,
    product_feats.num_reviews,
    product_feats.sum_ratings,
    product_feats.avg_rating,
    product_feats.std_rating,
    product_feats.min_rating,
    product_feats.max_rating,
    product_feats.pct_verified_reviews,
    product_feats.avg_review_length,
    product_feats.last_review_summary,
    product_feats.product_bias,
    reviewer_aggs.avg_reviewer_num_reviews,
    reviewer_aggs.avg_reviewer_total_spent,
    reviewer_aggs.avg_reviewer_avg_price,
    reviewer_aggs.avg_reviewer_avg_rating,
    reviewer_aggs.avg_reviewer_std_rating,
    trends.num_reviews_trend,
    trends.avg_rating_trend,
    trends.sum_ratings_trend,
    trends.min_rating_trend,
    trends.max_rating_trend,
    trends.avg_review_length_trend,
    trends.product_bias_trend
from labels
left join product_feats
    on
        labels.product_id = product_feats.product_id
        and labels.timestamp = product_feats.timestamp
left join reviewer_aggs
    on
        labels.product_id = reviewer_aggs.product_id
        and labels.timestamp = reviewer_aggs.timestamp
left join trends
    on
        labels.product_id = trends.product_id
        and labels.timestamp = trends.timestamp
