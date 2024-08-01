create or replace table user_attendance_{{ set }}_feats as -- noqa

with labels as materialized (
    -- driverId, date, position
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from user_attendance_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from user_attendance_{{ set }} -- noqa
    {% endif %}
),

friend_info as (
    select
        user_friends.user as user_id,
        count(distinct user_friends.friend) as num_friends
    from user_friends
    group by user_friends.user
),

user_feats as (
    select
        labels.user as user_id,
        labels.timestamp,
        users.locale,
        date_part('year', labels.timestamp) - users.birthyear as age,
        users.gender,
        date_diff('day', users.joinedAt, labels.timestamp) as days_on_app,
        users.location,
        users.timezone,
        friend_info.num_friends
    from labels
    left join users
        on labels.user = users.user_id
    left join friend_info
        on labels.user = friend_info.user_id
),

attendance_window_fns as materialized (
    select
        a.user_id,
        count(case when a.status == 'invited' then a.status end) over monthly as num_invited,
        count(case when a.status == 'yes' then a.status end) over monthly as num_yes,
        count(case when a.status == 'no' then a.status end) over monthly as num_no,
        count(case when a.status == 'maybe' then a.status end) over monthly as num_maybe,
        avg(date_part('hour', a.start_time)) over monthly as avg_event_start_hour,
        mode(date_part('dow', a.start_time)) over monthly as modal_event_dow
    from event_attendees as a
    window monthly as (
        partition by a.user_id
        order by a.start_time asc
        range between interval '1 month' preceding and current row
    )
),

interest_window_fns as materialized (
    select
        i.user_id,
        sum(i.invited) over monthly as num_invites,
        sum(i.interested) over monthly as num_interested,
        sum(i.not_interested) over monthly as num_not_interested,
        sum((i.invited::bool and i.interested::bool)::int) over monthly
        as num_invited_and_interested,
        sum((i.invited::bool and i.not_interested::bool)::int) over monthly
        as num_invited_and_not_interested
    from event_interest as i
    window monthly as (
        partition by i.user_id
        order by i.timestamp asc
        range between interval '1 month' preceding and current row
    )
)

select
    labels.user,
    labels.timestamp,
    {% if set != 'test' +%} -- noqa
        labels.target,
    {% endif %}
    user_feats.* exclude (user_feats.user_id, user_feats.timestamp),
{% for i in [1, 2, 3, 4, 5] %}
    past_{{ i }}_att.num_invited as past_{{ i }}_num_invited,
    past_{{ i }}_att.num_yes as past_{{ i }}_num_yes,
    past_{{ i }}_att.num_no as past_{{ i }}_num_no,
    past_{{ i }}_att.num_maybe as past_{{ i }}_num_maybe,
    past_{{ i }}_att.avg_event_start_hour as past_{{ i }}_avg_event_start_hour,
    past_{{ i }}_att.modal_event_dow as past_{{ i }}_modal_event_dow,
    past_{{ i }}_int.num_invites as past_{{ i }}_num_invites,
    past_{{ i }}_int.num_interested as past_{{ i }}_num_interested,
    past_{{ i }}_int.num_not_interested as past_{{ i }}_num_not_interested,
    past_{{ i }}_int.num_invited_and_interested as past_{{ i }}_num_invited_and_interested,
    past_{{ i }}_int.num_invited_and_not_interested as past_{{ i }}_num_invited_and_not_interested
{%- if not loop.last %},{% endif %} -- noqa
{% endfor %}
from labels
left join user_feats
    on
        labels.user = user_feats.user_id
        and labels.timestamp = user_feats.timestamp
{% for i in [1, 2, 3] %}
    asof left join attendance_window_fns as past_{{ i }}_att
        on
            labels.user = past_{{ i }}_att.user_id
            and labels.timestamp - interval '{{ i - 1 }} month' > past_{{ i }}_att.timestamp
    asof left join interest_window_fns as past_{{ i }}_int
        on
            labels.user = past_{{ i }}_int.user_id
            and labels.timestamp - interval '{{ i - 1 }} month' > past_{{ i }}_int.timestamp
{% endfor %}
