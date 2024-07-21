create or replace table user_ltv_{{ set }}_feats as -- noqa

with labels as materialized (
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from user_ltv_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from user_ltv_{{ set }} -- noqa
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

{% for lb, ub in [(0, 3), (3, 6), (6, 9)] %}
    window_feats_{{ lb }}_to_{{ ub }} as (
        select
            labels.customer_id,
            labels.timestamp,
            count(*) as num_reviews_{{ lb }}_to_{{ ub }},
            sum(review.rating) as sum_review_ratings_{{ lb }}_to_{{ ub }},
            avg(length(review.review_text)) as avg_review_length_{{ lb }}_to_{{ ub }},
            avg(review.rating) as avg_review_rating_{{ lb }}_to_{{ ub }},
            avg(review.verified::int) as pct_verified_reviews_{{ lb }}_to_{{ ub }},
            stddev(review.rating) as std_review_rating_{{ lb }}_to_{{ ub }},
            min(review.rating) as min_review_rating_{{ lb }}_to_{{ ub }},
            max(review.rating) as max_review_rating_{{ lb }}_to_{{ ub }},
            avg(product_feats.rating) as avg_reviewed_product_rating_{{ lb }}_to_{{ ub }},
            sum(product_feats.rating) as sum_reviewed_product_rating_{{ lb }}_to_{{ ub }},
            stddev(product_feats.rating) as std_reviewed_product_rating_{{ lb }}_to_{{ ub }},
            min(product_feats.rating) as min_reviewed_product_rating_{{ lb }}_to_{{ ub }},
            max(product_feats.rating) as max_reviewed_product_rating_{{ lb }}_to_{{ ub }},
            avg(product_feats.price) as avg_reviewed_product_price_{{ lb }}_to_{{ ub }},
            sum(product_feats.price) as sum_reviewed_product_price_{{ lb }}_to_{{ ub }},
            stddev(product_feats.price) as std_reviewed_product_price_{{ lb }}_to_{{ ub }},
            min(product_feats.price) as min_reviewed_product_price_{{ lb }}_to_{{ ub }},
            max(product_feats.price) as max_reviewed_product_price_{{ lb }}_to_{{ ub }},
            mode(product_feats.category) as reviewed_product_modal_category_{{ lb }}_to_{{ ub }}
        from labels
        left join review
            on
                labels.customer_id = review.customer_id
                and labels.timestamp - interval '{{ lb }} months' > review.review_time
                and labels.timestamp - interval '{{ ub }} months' <= review.review_time
        left join product_feats
            on
                review.product_id = product_feats.product_id
                and labels.timestamp = product_feats.timestamp
        group by labels.customer_id, labels.timestamp
    ),
{% endfor %}

all_time_feats as (
    select
        labels.customer_id,
        labels.timestamp,
        max(date_diff('weeks', review.review_time, labels.timestamp)) as weeks_since_first_review,
        min(date_diff('weeks', review.review_time, labels.timestamp)) as last_review_weeks_ago,
        arg_max(review.summary, review.review_time) as last_review_summary_text,
        arg_max(product_feats.title, review.review_time) as last_reviewed_product_title,
        arg_max(product_feats.category, review.review_time) as last_reviewed_product_category,
        arg_max(review.verified, review.review_time) as last_review_is_verified,
        count(*) as num_reviews,
        sum(review.rating) as sum_review_ratings,
        avg(length(review.review_text)) as avg_review_length,
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
        mode(product_feats.category) as reviewed_product_modal_category
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
)

select
    labels.customer_id,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.ltv,
    {% endif %}
    {% for lb, ub in [(0, 3), (3, 6), (6, 9)] %}
        window_feats_{{ lb }}_to_{{ ub }}.* exclude(customer_id, timestamp), -- noqa
    {% endfor %}
    all_time_feats.* exclude (customer_id, timestamp) -- noqa
from labels
left join window_feats_0_to_3
    on
        labels.customer_id = window_feats_0_to_3.customer_id
        and labels.timestamp = window_feats_0_to_3.timestamp
left join window_feats_3_to_6
    on
        labels.customer_id = window_feats_3_to_6.customer_id
        and labels.timestamp = window_feats_3_to_6.timestamp
left join window_feats_6_to_9
    on
        labels.customer_id = window_feats_6_to_9.customer_id
        and labels.timestamp = window_feats_6_to_9.timestamp
left join all_time_feats
    on
        labels.customer_id = all_time_feats.customer_id
        and labels.timestamp = all_time_feats.timestamp
