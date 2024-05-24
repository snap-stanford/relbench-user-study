create or replace table sales_{{ set }}_feats as -- noqa

with labels as materialized (
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from sales_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from sales_{{ set }} -- noqa
    {% endif %}
),

txn_window_fns as materialized (
    select
        t.customer_id,
        t.t_dat,
        (sum(t.price) over monthly) / 3 as rolling_monthly_purchase_amount,
        (count(*) over monthly) / 3 as rolling_monthly_purchase_count,
        date_diff(
            'week',
            lag(t.t_dat, 1) over (partition by t.customer_id order by t.t_dat asc),
            t.t_dat
        ) as weeks_since_last_purchase
    from transactions as t
    window monthly as (
        partition by t.customer_id
        order by t.t_dat asc
        range between interval '3 month' preceding and current row
    )
),

{% for w in [1, 2, 3, 4, 5] %}
    txn_window_aggs_{{ w }}_weeks_ago as (
        select
            labels.article_id,
            labels.timestamp,
            count(*) as num_sales_{{ w }}_weeks_ago,
            sum(t.price) as sold_amount_{{ w }}_weeks_ago,
            avg(t.price) as avg_price_{{ w }}_weeks_ago,
            count(distinct t.customer_id) as num_customers_{{ w }}_weeks_ago,
            -- aggs of buyer features
            avg(customer.age) as avg_buyer_age_{{ w }}_weeks_ago,
            avg(txn_window_fns.rolling_monthly_purchase_amount)
            as avg_monthly_purchase_amount_{{ w }}_weeks_ago,
            avg(txn_window_fns.rolling_monthly_purchase_count)
            as avg_monthly_purchase_count_{{ w }}_weeks_ago,
            avg(txn_window_fns.weeks_since_last_purchase)
            as avg_weeks_since_last_purchase_{{ w }}_weeks_ago
        from labels
        left join transactions as t
            on
                labels.article_id = t.article_id
                and labels.timestamp - interval '{{ w - 1 }} weeks' > t.t_dat
                and labels.timestamp - interval '{{ w }} weeks' <= t.t_dat
        left join customer
            on t.customer_id = customer.customer_id
        asof join txn_window_fns
            on
                t.customer_id = txn_window_fns.customer_id
                and labels.timestamp >= txn_window_fns.t_dat
        group by labels.article_id, labels.timestamp
    ){% if not loop.last %},{% endif %}
{% endfor %}

select
    labels.article_id,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.sales,
    {% endif %}
    -- date features
    date_part('week', labels.timestamp) as week_of_year,
    date_part('month', labels.timestamp) as month_of_year,
    date_part('day', labels.timestamp) as day_of_month,
    -- no dow b/c pred is always on Monday
    -- article features
    article.department_no,
    article.section_no,
    article.perceived_colour_master_id,
    -- window features
    {% for w in [1, 2, 3, 4, 5] %}
        txn_window_aggs_{{ w }}_weeks_ago.* exclude(article_id, timestamp)
        {%- if not loop.last %},{% endif %} -- noqa
    {% endfor %}
from labels
left join article
    on labels.article_id = article.article_id
{% for w in [1, 2, 3, 4, 5] %}
left join txn_window_aggs_{{ w }}_weeks_ago
    on
        labels.article_id = txn_window_aggs_{{ w }}_weeks_ago.article_id
        and labels.timestamp = txn_window_aggs_{{ w }}_weeks_ago.timestamp
{% endfor %}
