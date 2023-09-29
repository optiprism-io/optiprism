import requests

ch_addr = "http://localhost:8123"


def aggregate_property_query(agg, field, distinct=""):
    q = """select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, {0}({2}event_{1}) as sums
        from file('*.parquet', Parquet) as b
        where b.event_event = 'event'
          and toStartOfDay(event_created_at, 'UTC') >=
              toStartOfDay(now(), 'UTC') - INTERVAL 2 day
        group by c order by 1 asc format JSONCompactColumns;""".format(agg, field, distinct)

    resp = requests.get(ch_addr,
                        params={"query": q})

    ts = list(map(lambda x: x * 1000000000, resp.json()[0]))
    val = list(map(lambda x: float(x), resp.json()[1]))

    return [ts, val]


def aggregate_property_query_grouped(agg, field, group, distinct=""):
    q = ("""select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, event_{group}, {agg}({distinct}event_{field}) as sums
        from file('*.parquet', Parquet) as b
        where b.event_event = 'event'
          and toStartOfDay(event_created_at, 'UTC') >=
              toStartOfDay(now(), 'UTC') - INTERVAL 2 day
        group by c, event_{group} order by 1,2 asc format JSONCompactColumns;""".
         format(group=group, agg=agg, field=field,
                distinct=distinct))

    resp = requests.get(ch_addr,
                        params={"query": q})

    ts = list(map(lambda x: x * 1000000000, resp.json()[0]))
    group = list(map(lambda x: float(x), resp.json()[1]))
    val = list(map(lambda x: float(x), resp.json()[2]))

    return [ts, group, val]


def partitioned_aggregate_property_query(agg, outer_agg, field):
    q = """select c, {outer_agg}(counts)
        from (
                 select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, {inner_agg}(event_{field}) as counts
                 from file('*.parquet', Parquet) as b
                 where b.event_event = 'event'
                   and toStartOfDay(event_created_at, 'UTC') >=
                       toStartOfDay(now(), 'UTC') - INTERVAL 2 day
                 group by event_user_id,c)
        group by c 
        order by 1 asc format JSONCompactColumns;""".format(outer_agg=outer_agg, inner_agg=agg, field=field)

    ch_resp = requests.get(ch_addr,
                           params={"query": q})
    ch_ts = list(map(lambda x: x * 1000000000, ch_resp.json()[0]))
    ch_val = list(map(lambda x: float(x), ch_resp.json()[1]))
    return [ch_ts, ch_val]


def partitioned_aggregate_property_query_grouped(agg, outer_agg, field, group=''):
    q = """select c, {group}, {outer_agg}(counts)
        from (
                 select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, {group}, {inner_agg}(event_{field}) as counts
                 from file('*.parquet', Parquet) as b
                 where b.event_event = 'event'
                   and toStartOfDay(event_created_at, 'UTC') >=
                       toStartOfDay(now(), 'UTC') - INTERVAL 2 day
                 group by event_user_id, c,{group})
        group by c,{group} 
        order by 1 asc format JSONCompactColumns;""".format(outer_agg=outer_agg, inner_agg=agg, field=field,
                                                            group=group)

    ch_resp = requests.get(ch_addr,
                           params={"query": q})
    ch_ts = list(map(lambda x: x * 1000000000, ch_resp.json()[0]))
    ch_group = list(map(lambda x: float(x), ch_resp.json()[1]))
    ch_val = list(map(lambda x: float(x), ch_resp.json()[2]))
    return [ch_ts, ch_group, ch_val]
