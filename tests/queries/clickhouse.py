import requests

ch_addr = "http://localhost:8123"


def aggregate_property_query(agg, field, group=None, distinct=False, interval="day", period=2, period_interval="day"):
    g = "c"
    if group is not None:
        g = "c, {0}".format(group)

    d = ""
    if distinct is True:
        d = "distinct "

    q = """select toUnixTimestamp(date_trunc('{interval}',created_at, 'UTC')) as {group}, {agg}({distinct}{field}) as sums
        from file('store/tables/*/*/*/*.parquet', Parquet) as b
        where b.event = 4
          and created_at >=
              now() - INTERVAL {period} {period_interval}
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
    if interval == "minute":
        i = "toMinute"
    elif interval == "hour":
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
        g = ", {0}".format(group)
    q = """select c, {group} {outer_agg}(counts)
        from (
                 select toUnixTimestamp(date_trunc('{interval}',created_at, 'UTC')) as c, {group} {inner_agg}({field}) as counts
                 from file('store/tables/*/*/*/*.parquet', Parquet) as b
                 where b.event = 4
                   and created_at >=
                       now() - INTERVAL {period} {period_interval}
                 group by user_id,c {group})
        group by c {group}
        order by 1 asc format JSONCompactColumns;""".format(outer_agg=outer_agg, inner_agg=agg, field=field,
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


def all_aggregates_query(field, group=None, interval="day", period=2, period_interval="day"):
    g = "c"
    if group is not None:
        g = "c, {0}".format(group)

    q = """select toUnixTimestamp(date_trunc('{interval}',created_at, 'UTC')) as {group}, 
        count({field}), min({field}), max({field}), avg({field}), sum({field})
        from file('store/tables/*/*/*/*.parquet', Parquet) as b
        where b.event = 4
          and created_at >=
              now() - INTERVAL {period} {period_interval}
        group by {group} order by {group} asc format JSONCompactColumns;""".format(field=field,
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
