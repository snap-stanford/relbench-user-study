create or replace table churn_{{ set }}_feats as -- noqa

with labels as (
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from churn_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from churn_{{ set }} -- noqa
    {% endif %}
),

timestamps as (
    select distinct timestamp from labels
),

product_ratings as (
    select
        timestamps.timestamp,
        review.product_id,
        avg(review.rating) as rating,
        stddev(review.rating) as rating_std
    from timestamps
    left join review
        on timestamps.timestamp > review.review_time
    group by all
),

product_feats as (
    select
        product.product_id,
        product_ratings.timestamp,
        product.price,
        product.title,
        product_ratings.rating,
        product_ratings.rating_std,
        product.category[-1] as category
    from product
    left join product_ratings
        on product.product_id = product_ratings.product_id
),

review_aggs as (
    select
        labels.customer_id,
        labels.timestamp,
        max(date_diff('weeks', review.review_time, labels.timestamp)) as weeks_since_first_review,
        count(*) as num_reviews,
        sum(review.rating) as sum_review_ratings,
        avg(length(review.summary)) as avg_review_length,
        min(date_diff('weeks', review.review_time, labels.timestamp)) as last_review_weeks_ago,
        arg_max(review.summary, review.review_time) as last_review_summary_text,
        arg_max(product_feats.title, review.review_time) as last_reviewed_product_title,
        arg_max(product_feats.category, review.review_time) as last_reviewed_product_category,
        arg_max(review.verified, review.review_time) as last_review_is_verified,
        avg(review.rating) as avg_review_rating,
        avg(review.verified::int) as pct_verified_reviews,
        stddev(review.rating) as std_review_rating,
        min(review.rating) as min_review_rating,
        max(review.rating) as max_review_rating,
        avg(product_feats.rating) as avg_reviewed_product_rating,
        sum(product_feats.rating) as sum_reviewed_product_rating,
        stddev(product_feats.rating) as std_reviewed_product_rating,
        min(product_feats.rating) as min_reviewed_product_rating,
        max(product_feats.rating) as max_reviewed_product_rating,
        avg(product_feats.price) as avg_reviewed_product_price,
        sum(product_feats.price) as sum_reviewed_product_price,
        stddev(product_feats.price) as std_reviewed_product_price,
        min(product_feats.price) as min_reviewed_product_price,
        max(product_feats.price) as max_reviewed_product_price,
        mode(product_feats.category) as reviewed_product_modal_category,
        avg((review.rating - product_feats.rating) / product_feats.rating_std) as user_bias
    from labels
    left join review
        on
            labels.customer_id = review.customer_id
            and labels.timestamp > review.review_time
    left join product_feats
        on
            review.product_id = product_feats.product_id
            and labels.timestamp = product_feats.timestamp
    group by labels.customer_id, labels.timestamp
),

last_6mo as (
    select
        labels.customer_id,
        labels.timestamp,
        count(*) as num_reviews,
        avg(review.rating) as avg_review_rating,
        avg(product_feats.price) as avg_price,
        avg((review.rating - product_feats.rating) / product_feats.rating_std) as user_bias
    from labels
    left join review
        on
            labels.customer_id = review.customer_id
            and labels.timestamp > review.review_time
            and (labels.timestamp - interval '6 months') < review.review_time
    left join product_feats
        on
            review.product_id = product_feats.product_id
            and labels.timestamp = product_feats.timestamp
    group by labels.customer_id, labels.timestamp
),

prev_6mo as (
    select
        labels.customer_id,
        labels.timestamp,
        count(*) as num_reviews,
        avg(review.rating) as avg_review_rating,
        avg(product_feats.price) as avg_price,
        avg((review.rating - product_feats.rating) / product_feats.rating_std) as user_bias
    from labels
    left join review
        on
            labels.customer_id = review.customer_id
            and (labels.timestamp - interval '6 months') > review.review_time
            and (labels.timestamp - interval '12 months') < review.review_time
    left join product_feats
        on
            review.product_id = product_feats.product_id
            and labels.timestamp = product_feats.timestamp
    group by labels.customer_id, labels.timestamp
),

trends as (
    select
        labels.customer_id,
        labels.timestamp,
        (last_6mo.num_reviews - prev_6mo.num_reviews)
        / coalesce(prev_6mo.num_reviews, 1e-1) as num_reviews_trend,
        (last_6mo.avg_review_rating - prev_6mo.avg_review_rating)
        / coalesce(prev_6mo.avg_review_rating, 1e-1) as avg_rating_trend,
        (last_6mo.avg_price - prev_6mo.avg_price)
        / coalesce(prev_6mo.avg_price, 1e-1) as avg_price_trend,
        (last_6mo.user_bias - prev_6mo.user_bias)
        / coalesce(prev_6mo.user_bias, 1e-1) as avg_user_bias_trend
    from labels
    left join last_6mo
        on
            labels.customer_id = last_6mo.customer_id
            and labels.timestamp = last_6mo.timestamp
    left join prev_6mo
        on
            labels.customer_id = prev_6mo.customer_id
            and labels.timestamp = prev_6mo.timestamp
)

select
    labels.customer_id,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.churn,
    {% endif %}
    review_aggs.weeks_since_first_review,
    review_aggs.num_reviews,
    review_aggs.sum_review_ratings,
    review_aggs.avg_review_length,
    review_aggs.last_review_weeks_ago,
    review_aggs.last_review_summary_text,
    review_aggs.last_reviewed_product_title,
    review_aggs.last_reviewed_product_category,
    review_aggs.last_review_is_verified,
    review_aggs.avg_review_rating,
    review_aggs.pct_verified_reviews,
    review_aggs.std_review_rating,
    review_aggs.min_review_rating,
    review_aggs.max_review_rating,
    review_aggs.avg_reviewed_product_rating,
    review_aggs.sum_reviewed_product_rating,
    review_aggs.std_reviewed_product_rating,
    review_aggs.min_reviewed_product_rating,
    review_aggs.max_reviewed_product_rating,
    review_aggs.avg_reviewed_product_price,
    review_aggs.sum_reviewed_product_price,
    review_aggs.std_reviewed_product_price,
    review_aggs.min_reviewed_product_price,
    review_aggs.max_reviewed_product_price,
    review_aggs.reviewed_product_modal_category,
    review_aggs.user_bias,
    trends.num_reviews_trend,
    trends.avg_rating_trend,
    trends.avg_price_trend,
    trends.avg_user_bias_trend
from labels
left join review_aggs
    on
        labels.customer_id = review_aggs.customer_id
        and labels.timestamp = review_aggs.timestamp
left join trends
    on
        labels.customer_id = trends.customer_id
        and labels.timestamp = trends.timestamp
