create or replace table user_churn_{{ set }}_feats as -- noqa

with labels as materialized (
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from user_churn_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from user_churn_{{ set }} -- noqa
    {% endif %}
),

txn_window_fns as materialized (
    select
        t.article_id,
        t.t_dat,
        (sum(t.price) over monthly) / 3 as rolling_monthly_sales_amount,
        (count(*) over monthly) / 3 as rolling_monthly_sales_count,
        date_diff(
            'days',
            lag(t.t_dat, 1) over (partition by t.customer_id order by t.t_dat asc),
            t.t_dat
        ) as days_since_last_sale
    from transactions as t
    window monthly as (
        partition by t.article_id
        order by t.t_dat asc
        range between interval '3 month' preceding and current row
    )
),

-- use window function for all time aggregates to avoid costly join
txns_all_time_window as materialized (
    select
        t.customer_id,
        t.t_dat,
        count(*) over all_time as total_purchase_count,
        sum(t.price) over all_time as total_purchase_amount,
        avg(t.price) over all_time as avg_purchase_price,
        count(distinct t.article_id) over all_time as total_unique_articles_purchased,
        avg(t.sales_channel_id - 1) over all_time as prop_sales_channel_2,
        mode(article.department_no) over all_time as modal_dept_no,
        mode(article.section_no) over all_time as modal_section_no,
        mode(article.perceived_colour_master_id) over all_time as modal_color_id
    from transactions as t
    left join article
        on t.article_id = article.article_id
    window all_time as (
        partition by t.customer_id
        order by t.t_dat asc
        range between unbounded preceding and current row
    )
),

{% for w in [1, 2, 3, 4, 5] %}
    txn_window_aggs_{{ w }}_weeks_ago as (
        select
            labels.customer_id,
            labels.timestamp,
            count(*) as num_purchases_{{ w }}_weeks_ago,
            sum(t.price) as purchased_amount_{{ w }}_weeks_ago,
            avg(t.price) as avg_purchase_price_{{ w }}_weeks_ago,
            count(distinct t.article_id) as num_unique_articles_purchased_{{ w }}_weeks_ago,
            avg(t.sales_channel_id - 1) as prop_sales_channel_2_{{ w }}_weeks_ago,
            -- aggs of purchased item features
            avg(txn_window_fns.rolling_monthly_sales_amount)
            as avg_monthly_sales_amount_{{ w }}_weeks_ago,
            avg(txn_window_fns.rolling_monthly_sales_count)
            as avg_monthly_sales_count_{{ w }}_weeks_ago,
            avg(txn_window_fns.days_since_last_sale)
            as avg_days_since_last_sale_{{ w }}_weeks_ago,
            mode(article.department_no) as modal_dept_no_{{ w }}_weeks_ago,
            mode(article.section_no) as modal_section_no_{{ w }}_weeks_ago,
            mode(article.perceived_colour_master_id) as modal_color_id_{{ w }}_weeks_ago
        from labels
        left join transactions as t
            on
                labels.customer_id = t.customer_id
                and labels.timestamp - interval '{{ w - 1 }} weeks' > t.t_dat
                and labels.timestamp - interval '{{ w }} weeks' <= t.t_dat
        left join article
            on t.article_id = article.article_id
        asof left join txn_window_fns
            on
                t.article_id = txn_window_fns.article_id
                and labels.timestamp > txn_window_fns.t_dat
        group by labels.customer_id, labels.timestamp
    ){% if not loop.last %},{% endif %}
{% endfor %}

select
    labels.customer_id,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.churn,
    {% endif %}
    -- date features
    date_part('week', labels.timestamp) as week_of_year,
    date_part('month', labels.timestamp) as month_of_year,
    date_part('day', labels.timestamp) as day_of_month,
    -- no dow b/c pred is always on Monday
    -- customer features
    customer.age,
    (customer.FN is not null)::int as fn_not_null,
    (customer.Active is not null)::int as is_active,
    customer.club_member_status,
    customer.fashion_news_frequency,
    -- article features
    all_time.* exclude(customer_id, t_dat),
    {% for w in [1, 2, 3, 4, 5] %}
        txn_window_aggs_{{ w }}_weeks_ago.* exclude(customer_id, timestamp)
        {%- if not loop.last %},{% endif %} -- noqa
    {% endfor %}
from labels
left join customer
    on labels.customer_id = customer.customer_id
asof left join txns_all_time_window as all_time
    on
        labels.customer_id = all_time.customer_id
        and labels.timestamp > all_time.t_dat
{% for w in [1, 2, 3, 4, 5] %}
left join txn_window_aggs_{{ w }}_weeks_ago
    on
        labels.customer_id = txn_window_aggs_{{ w }}_weeks_ago.customer_id
        and labels.timestamp = txn_window_aggs_{{ w }}_weeks_ago.timestamp
{% endfor %}
