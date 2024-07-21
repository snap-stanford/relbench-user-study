create or replace table item_ltv_{{ set }}_feats as -- noqa

with labels as materialized (
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from item_ltv_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from item_ltv_{{ set }} -- noqa
    {% endif %}
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

{% for lb, ub in [(0, 3), (3, 6), (6, 9)] %}
    window_feats_{{ lb }}_to_{{ ub }} as (
        select
            labels.product_id,
            labels.timestamp,
            count(*) as num_reviews_{{ lb }}_to_{{ ub }},
            sum(review.rating) as sum_ratings_{{ lb }}_to_{{ ub }},
            avg(review.rating) as avg_rating_{{ lb }}_to_{{ ub }},
            stddev(review.rating) as std_rating_{{ lb }}_to_{{ ub }},
            min(review.rating) as min_rating_{{ lb }}_to_{{ ub }},
            max(review.rating) as max_rating_{{ lb }}_to_{{ ub }},
            avg(length(review.review_text)) as avg_review_length_{{ lb }}_to_{{ ub }},
            avg(review.verified::int) as pct_verified_reviews_{{ lb }}_to_{{ ub }},
            (avg(review.rating) - avg(avg(review.rating)) over ())
            / stddev(avg(review.rating)) over () as product_bias_{{ lb }}_to_{{ ub }},
            -- reviewer aggs: too slow to compute customer features as of window end date so using
            -- features as of timestamp (should be close enough)
            avg(customer_feats.num_reviews) as avg_reviewer_num_reviews_{{ lb }}_to_{{ ub }},
            avg(customer_feats.total_spent) as avg_reviewer_total_spent_{{ lb }}_to_{{ ub }},
            avg(customer_feats.avg_price) as avg_reviewer_avg_price_{{ lb }}_to_{{ ub }},
            avg(customer_feats.avg_rating) as avg_reviewer_avg_rating_{{ lb }}_to_{{ ub }},
            avg(customer_feats.std_rating) as avg_reviewer_std_rating_{{ lb }}_to_{{ ub }}
        from labels
        left join review
            on
                labels.product_id = review.product_id
                and labels.timestamp - interval '{{ lb }} months' > review.review_time
                and labels.timestamp - interval '{{ ub }} months' <= review.review_time
        left join customer_feats
            on
                review.customer_id = customer_feats.customer_id
                and labels.timestamp = customer_feats.timestamp
        group by labels.product_id, labels.timestamp
    ),
{% endfor %}

-- TODO issue is in this table!
all_time_feats as (
    select
        labels.product_id,
        labels.timestamp,
        max(date_diff('weeks', review.review_time, labels.timestamp)) as weeks_since_first_review,
        median(
            date_diff('weeks', review.review_time, labels.timestamp)
        ) as weeks_since_median_review,
        min(date_diff('weeks', review.review_time, labels.timestamp)) as weeks_since_latest_review,
        arg_max(review.summary, review.review_time) as last_review_summary,
        arg_max(review.verified::int, review.review_time) as last_review_is_verified,
        count(*) as num_reviews,
        sum(review.rating) as sum_ratings,
        avg(review.rating) as avg_rating,
        stddev(review.rating) as std_rating,
        min(review.rating) as min_rating,
        max(review.rating) as max_rating,
        avg(length(review.review_text)) as avg_review_length,
        avg(review.verified::int) as pct_verified_reviews,
        (avg(review.rating) - avg(avg(review.rating)) over ())
        / stddev(avg(review.rating)) over () as product_bias,
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
    group by labels.product_id, labels.timestamp
)

select
    labels.product_id,
    labels.timestamp,
    product.price,
    product.category[-1] as category,
    product.title,
    {% if set != 'test' +%} -- noqa
        labels.ltv,
    {% endif %}
    {% for lb, ub in [(0, 3), (3, 6), (6, 9)] %}
        window_feats_{{ lb }}_to_{{ ub }}.* exclude(product_id, timestamp), -- noqa
    {% endfor %}
    all_time_feats.* exclude (product_id, timestamp) -- noqa
from labels
left join product
    on labels.product_id = product.product_id
left join window_feats_0_to_3
    on
        labels.product_id = window_feats_0_to_3.product_id
        and labels.timestamp = window_feats_0_to_3.timestamp
left join window_feats_3_to_6
    on
        labels.product_id = window_feats_3_to_6.product_id
        and labels.timestamp = window_feats_3_to_6.timestamp
left join window_feats_6_to_9
    on
        labels.product_id = window_feats_6_to_9.product_id
        and labels.timestamp = window_feats_6_to_9.timestamp
left join all_time_feats
    on
        labels.product_id = all_time_feats.product_id
        and labels.timestamp = all_time_feats.timestamp
