create or replace table dnf_{{ set }}_feats as -- noqa

with labels as materialized (
    -- driverId, date, dnf
    {% if (set == 'train') and (subsample > 0) %} -- noqa
    select * from dnf_{{ set }} using sample {{ subsample }} -- noqa
    {% else %}
    select * from dnf_{{ set }} -- noqa
    {% endif %}
),

standings2 as (
    select
        *,
        points - lag(points, 1) over (partition by raceId order by position asc) as points_lag,
        points - lead(points, 1) over (partition by raceId order by position asc) as points_lead
    from standings
),

constructor_standings2 as (
    select
        *,
        points - lag(points, 1) over (partition by raceId order by position asc) as points_lag,
        points - lead(points, 1) over (partition by raceId order by position asc) as points_lead
    from constructor_standings
),

-- results has duplicates
results_dd as materialized (
    select distinct on (raceId, driverId)
        raceId,
        driverId,
        constructorId,
        position,
        points,
        grid,
        rank,
        laps,
        statusId,
        date
    from results
),

basic_feats as (
    select
        labels.driverId,
        labels.date,
        date_part('week', labels.date) as week_of_year,
        -- driver features
        drivers.driverRef as driver_ref, -- use as categorical
        date_diff('year', drivers.dob::date, labels.date) as driver_age,
        drivers.nationality as driver_nationality,
        -- driver standings
        standings2.position as driver_position,
        standings2.points as driver_points,
        standings2.wins as driver_wins,
        standings2.points_lag as driver_points_lag,
        standings2.points_lead as driver_points_lead,
        date_diff('day', standings2.date, labels.date) as days_since_last_race,
        -- constructor features
        constructors.constructorRef as constructor_ref,
        constructors.nationality as constructor_nationality,
        -- constructor standings
        constructor_standings2.position as constructor_position,
        constructor_standings2.points as constructor_points,
        constructor_standings2.wins as constructor_wins,
        constructor_standings2.points_lag as constructor_points_lag,
        constructor_standings2.points_lead as constructor_points_lead,
        -- driver contribution
        driver_position - constructor_position as position_diff,
        driver_points / nullif(constructor_points, 0) as points_ratio,
        driver_wins / nullif(constructor_wins, 0) as wins_ratio
    from labels
    left join drivers
        on labels.driverId = drivers.driverId
    asof left join standings2
        on
            labels.driverId = standings2.driverId
            and labels.date > standings2.date
    left join results_dd -- use it to join constructor_standings
        on
            standings2.raceId = results_dd.raceId
            and labels.driverId = results_dd.driverId
    left join constructor_standings2
        on
            standings2.raceId = constructor_standings2.raceId
            and results_dd.constructorId = constructor_standings2.constructorId
    left join constructors
        on constructor_standings2.constructorId = constructors.constructorId
),

past_race_features as (
    select
        labels.driverId,
        labels.date,
        (last_race.raceId - results_dd.raceId + 1) as races_ago,
        -- results_dd
        results_dd.position as driver_position,
        results_dd.points as driver_points,
        results_dd.grid as driver_grid,
        results_dd.position - results_dd.grid as position_gain,
        results_dd.rank as driver_rank,
        results_dd.laps
        / max(results_dd.laps) over (partition by results_dd.raceId) as pct_laps_completed,
        (results_dd.statusId != 1)::int as dnf,
        -- constructor results_dd
        constructor_results.points as constructor_points
    from labels
    asof left join races as last_race
        on labels.date > last_race.date
    left join results_dd
        on
            labels.driverId = results_dd.driverId
            and (last_race.raceId - results_dd.raceId) between 0 and 2 -- last 3 races
            -- exclude races from last season
            and results_dd.date >= (labels.date - interval '2 month')
    left join constructor_results
        on
            results_dd.raceId = constructor_results.raceId
            and results_dd.constructorId = constructor_results.constructorId
),

upcoming_race_features as (
    select
        labels.driverId,
        labels.date,
        (upcoming.raceId - last_race.raceId) as races_ahead,
        upcoming.round,
        circuits.circuitId
    from labels
    asof left join races as last_race
        on labels.date > last_race.date
    left join races as upcoming
        on
            (upcoming.raceId - last_race.raceId) between 1 and 3 -- next 3 races
            and upcoming.date <= (labels.date + interval '1 month')
    left join circuits
        on upcoming.circuitId = circuits.circuitId
)

select
    labels.driverId,
    labels.date,
    {% if set != 'test' +%} -- noqa
        labels.did_not_finish,
    {% endif %}
    basic_feats.* exclude(driverId, date),
    {% for i in [1, 2, 3] %}
        past_{{ i }}.driver_position as past_{{ i }}_driver_position,
        past_{{ i }}.driver_points as past_{{ i }}_driver_points,
        past_{{ i }}.driver_grid as past_{{ i }}_driver_grid,
        past_{{ i }}.position_gain as past_{{ i }}_position_gain,
        past_{{ i }}.driver_rank as past_{{ i }}_driver_rank,
        past_{{ i }}.pct_laps_completed as past_{{ i }}_pct_laps_completed,
        past_{{ i }}.dnf as past_{{ i }}_dnf,
        past_{{ i }}.constructor_points as past_{{ i }}_constructor_points,
        upcoming_{{ i }}.round as upcoming_{{ i }}_round,
        upcoming_{{ i }}.circuitId as upcoming_{{ i }}_circuit_id
        {%- if not loop.last %},{% endif %} -- noqa
    {% endfor %}
from labels
left join basic_feats
    on
        labels.driverId = basic_feats.driverId
        and labels.date = basic_feats.date
{% for i in [1, 2, 3] %}
    left join past_race_features as past_{{ i }}
        on
            labels.driverId = past_{{ i }}.driverId
            and labels.date = past_{{ i }}.date
            and past_{{ i }}.races_ago = {{ i }}
    left join upcoming_race_features as upcoming_{{ i }}
        on
            labels.driverId = upcoming_{{ i }}.driverId
            and labels.date = upcoming_{{ i }}.date
            and upcoming_{{ i }}.races_ahead = {{ i }}
{% endfor %}
