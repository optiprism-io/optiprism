import requests

ch_addr = "http://localhost:8123"


def aggregate_property_query(agg, field, group=None, distinct=False, interval="day", period=2, period_interval="day"):
    g = "c"
    if group is not None:
        g = "c, event_{0}".format(group)

    d = ""
    if distinct is True:
        d = "distinct "

    q = """select toUnixTimestamp(date_trunc('{interval}',event_created_at, 'UTC')) as {group}, {agg}({distinct}event_{field}) as sums
        from file('*.parquet', Parquet) as b
        where b.event_event = 'event'
          and event_created_at >=
              date_trunc('{interval}',now(),'UTC') - INTERVAL {period} {period_interval}
        group by {group} order by {group} asc format JSONCompactColumns;""".format(agg=agg, field=field, distinct=d,
                                                                                   period=period,
                                                                                   period_interval=period_interval,
                                                                                   interval=interval,
                                                                                   group=g)

    print(q)
    resp = requests.get(ch_addr,
                        params={"query": q})

    if not resp.json():
        return []

    ts = list(map(lambda x: x * 1000000000, resp.json()[0]))

    if group is None:
        val = list(map(lambda x: float(x), resp.json()[1]))
        return [ts, val]
    else:
        group = list(map(lambda x: float(x), resp.json()[1]))
        val = list(map(lambda x: float(x), resp.json()[2]))
        return [ts, group, val]


def partitioned_aggregate_property_query(agg, outer_agg, field, group=None, interval="day", period=2,
                                         period_interval="day"):
    i = ""
    if interval == "hour":
        i = "toHour"
    elif interval == "day":
        i = "toDate"
    elif interval == "week":
        i = "toStartOfWeek"
    elif interval == "month":
        i = "toStartOfMonth"
    elif interval == "year":
        i = "toStartOfYear"

    g = ""
    if group is not None:
        g = ", event_{0}".format(group)
    q = """select c, {group} {outer_agg}(counts)
        from (
                 select toUnixTimestamp({interval}(event_created_at, 'UTC')) as c, {group} {inner_agg}(event_{field}) as counts
                 from file('*.parquet', Parquet) as b
                 where b.event_event = 'event'
                   and toStartOfDay(event_created_at, 'UTC') >=
                       toStartOfDay(now(), 'UTC') - INTERVAL {period} {period_interval}
                 group by event_user_id,c {group})
        group by c {group}
        order by 1 asc format JSONCompactColumns;""".format(outer_agg=outer_agg, inner_agg=agg, field=field,
                                                            period=period,
                                                            period_interval=period_interval,
                                                            interval=i,
                                                            group=g)

    resp = requests.get(ch_addr,
                        params={"query": q})
    ts = list(map(lambda x: x * 1000000000, resp.json()[0]))

    if group is None:
        val = list(map(lambda x: float(x), resp.json()[1]))
        return [ts, val]
    else:
        group = list(map(lambda x: float(x), resp.json()[1]))
        val = list(map(lambda x: float(x), resp.json()[2]))
        return [ts, group, val]
