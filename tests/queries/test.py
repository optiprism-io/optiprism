import requests

ch_addr = "http://localhost:8123"
op_addr = "http://localhost:8080/api/v1"

aggs = ["min", "max", "avg", "sum", "count"]
fields = ["i_8", "i_16", "i_32", "i_64", "u_8", "u_16", "u_32", "u_64", "f_32", "f_64", "decimal"]


def auth():
    auth_body = {
        "email": "admin@email.com",
        "password": "admin"
    }

    auth_resp = requests.post(op_addr + "/auth/login", json=auth_body, headers={"Content-Type": "application/json"})

    return auth_resp.json()['accessToken']


token = auth()


def agg_prop_ch_query(agg, field, distinct=""):
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

    print(val)
    return [ts, val]


def agg_prop_op_query(agg, field: str, prop_type="event"):
    q = {
        "time": {
            "type": "last",
            "last": 3,
            "unit": "day"
        },
        "group": "user",
        "intervalUnit": "day",
        "chartType": "line",
        "analysis": {
            "type": "linear"
        },
        "events": [
            {
                "eventName": "event",
                "queries": [
                    {
                        "type": "aggregateProperty",
                        "aggregate": agg,
                        "propertyType": prop_type,
                        "propertyName": field
                    },
                ],
                "eventType": "regular",
                "eventId": 8,
                "filters": []
            }
        ],
        "filters": {
            "groupsCondition": "and",
            "groups": []
        },
        "segments": [],
        "breakdowns": []
    }

    resp = requests.post(
        "{0}/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact".format(op_addr),
        json=q,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + token})

    ts = resp.json()[1]
    val = resp.json()[2]

    return [ts, val]


def simple_op_query(query: str):
    q = {
        "time": {
            "type": "last",
            "last": 3,
            "unit": "day"
        },
        "group": "user",
        "intervalUnit": "day",
        "chartType": "line",
        "analysis": {
            "type": "linear"
        },
        "events": [
            {
                "eventName": "event",
                "queries": [
                    {
                        "type": query
                    },
                ],
                "eventType": "regular",
                "eventId": 8,
                "filters": []
            }
        ],
        "filters": {
            "groupsCondition": "and",
            "groups": []
        },
        "segments": [],
        "breakdowns": []
    }

    resp = requests.post(
        "{0}/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact".format(op_addr),
        json=q,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + token})

    ts = resp.json()[1]
    val = resp.json()[2]

    return [ts, val]


def assert_agg_prop(agg, field: str):
    assert agg_prop_ch_query(agg, field) == agg_prop_op_query(agg, field)


def partitioned_agg_prop_ch_query(agg, outer_agg, field):
    q = """select c, {0}(counts)
        from (
                 select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, {1}(event_{2}) as counts
                 from file('*.parquet', Parquet) as b
                 where b.event_event = 'event'
                   and toStartOfDay(event_created_at, 'UTC') >=
                       toStartOfDay(now(), 'UTC') - INTERVAL 2 day
                 group by event_user_id, c)
        group by c
        order by 1 asc format JSONCompactColumns;""".format(outer_agg, agg, field)

    ch_resp = requests.get(ch_addr,
                           params={"query": q})
    ch_ts = list(map(lambda x: x * 1000000000, ch_resp.json()[0]))
    ch_val = list(map(lambda x: float(x), ch_resp.json()[1]))
    return [ch_ts, ch_val]


def partitioned_agg_prop_op_query(agg, outer_agg, field: str, prop_type="event"):
    q = {
        "time": {
            "type": "last",
            "last": 3,
            "unit": "day"
        },
        "group": "user",
        "intervalUnit": "day",
        "chartType": "line",
        "analysis": {
            "type": "linear"
        },
        "events": [
            {
                "eventName": "event",
                "queries": [
                    {
                        "type": "aggregatePropertyPerGroup",
                        "aggregate": outer_agg,
                        "aggregatePerGroup": agg,
                        "propertyType": prop_type,
                        "propertyName": field
                    },
                ],
                "eventType": "regular",
                "eventId": 8,
                "filters": []
            }
        ],
        "filters": {
            "groupsCondition": "and",
            "groups": []
        },
        "segments": [],
        "breakdowns": []
    }

    resp = requests.post(
        "{0}/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact".format(op_addr),
        json=q,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + token})

    ts = resp.json()[1]
    val = resp.json()[2]

    return [ts, val]


def assert_partitioned_agg_prop(agg, outer_agg, field: str):
    assert partitioned_agg_prop_ch_query(agg, outer_agg, field) == partitioned_agg_prop_op_query(agg, outer_agg, field)


def test_count_events():
    assert agg_prop_ch_query("count", "event") == simple_op_query("countEvents")


def test_count_unique_groups():
    assert agg_prop_ch_query("uniq", "user_id", "") == simple_op_query("countUniqueGroups")


def test_agg_prop():
    for field in fields:
        for agg in aggs:
            print("Test Aggregate Property {0}({1})".format(agg, field))
            assert_agg_prop(agg, field)


def test_partitioned_count():
    for agg in ["min", "max", "avg", "sum"]:
        print("Test Partitioned Count ({0})".format(agg))
        ch_q = """select c, {0}(counts)
        from (
                 select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, count(1) as counts
                 from file('*.parquet', Parquet) as b
                 where b.event_event = 'event'
                   and toStartOfDay(event_created_at, 'UTC') >=
                       toStartOfDay(now(), 'UTC') - INTERVAL 2 day
                 group by event_user_id, c)
        group by c
        order by 1 asc format JSONCompactColumns;""".format(agg)

        ch_resp = requests.get(ch_addr,
                               params={"query": ch_q})
        ch_ts = list(map(lambda x: x * 1000000000, ch_resp.json()[0]))
        ch_val = list(map(lambda x: float(x), ch_resp.json()[1]))

        op_query = {
            "time": {
                "type": "last",
                "last": 3,
                "unit": "day"
            },
            "group": "user",
            "intervalUnit": "day",
            "chartType": "line",
            "analysis": {
                "type": "linear"
            },
            "events": [
                {
                    "eventName": "event",
                    "queries": [
                        {
                            "type": "countPerGroup",
                            "aggregate": agg
                        },
                    ],
                    "eventType": "regular",
                    "eventId": 8,
                    "filters": []
                }
            ],
            "filters": {
                "groupsCondition": "and",
                "groups": []
            },
            "segments": [],
            "breakdowns": []
        }

        op_resp = requests.post(
            "{0}/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact".format(op_addr),
            json=op_query,
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + token})

        print(op_resp.json())
        op_ts = op_resp.json()[1]
        op_val = op_resp.json()[2]

        assert ch_ts == op_ts
        assert ch_val == op_val


def return_type(typ, agg):
    if agg == "min" or agg == "max":
        return typ
    elif agg == "count":
        return "i64"
    elif agg == "avg":
        return "f64"
    else:
        if typ == "i8":
            return "i64"
        elif typ == "i64":
            return "i128"
        elif typ == "f64":
            return "f64"
        return "casted"


def test_agg_combinations():
    print()
    for agg in aggs:
        rt1 = return_type("i8", agg)
        print("{0}: {1}".format(agg, rt1))
        for agg2 in aggs:
            rt2 = return_type(rt1, agg2)
            print("  {0}: {1}".format(agg2, rt2))
