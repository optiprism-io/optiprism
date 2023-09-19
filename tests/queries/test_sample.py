import json

import pytest
import requests
from attr import dataclass


@dataclass
class Query:
    ch_query: str
    op_query: object


ch_addr = "http://localhost:8123"
op_addr = "http://localhost:8080/api/v1"


def get_ch_last_time():
    q = """"""
def agg_prop_ch_query(agg, field):
    q = """select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, {0}({1}) as sums
        from file('*.parquet', Parquet) as b
        where b.event_event = 'Order Completed'
          and toStartOfDay(event_created_at, 'UTC') >=
              toStartOfDay(now(), 'UTC') - INTERVAL 2 day
        group by c order by 1 asc format JSONCompactColumns;""".format(agg, field)

    resp = requests.get(ch_addr,
                        params={"query": q})

    ts = list(map(lambda x: x * 1000000000, resp.json()[0]))
    val = list(map(lambda x: float(x), resp.json()[1]))

    print(val)
    return [ts, val]


def agg_prop_op_query(agg, field: str):
    parts = field.split("_")
    prop_type = parts[0]
    prop_name: str = " ".join(list(map(lambda p: p.capitalize(), parts[1:])))

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
                "eventName": "Order Completed",
                "queries": [
                    {
                        "type": "aggregateProperty",
                        "aggregate": agg,
                        "propertyType": prop_type,
                        "propertyName": prop_name
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

    token = auth()
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
                "eventName": "Order Completed",
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

    token = auth()
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
                 select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, {1}({2}) as counts
                 from file('*.parquet', Parquet) as b
                 where b.event_event = 'Order Completed'
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


def partitioned_agg_prop_op_query(agg, outer_agg, field: str):
    parts = field.split("_")
    prop_type = parts[0]
    prop_name: str = " ".join(list(map(lambda p: p.capitalize(), parts[1:])))

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
                "eventName": "Order Completed",
                "queries": [
                    {
                        "type": "aggregatePropertyPerGroup",
                        "aggregate": outer_agg,
                        "aggregatePerGroup": agg,
                        "propertyType": prop_type,
                        "propertyName": prop_name
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

    token = auth()
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


def auth():
    auth_body = {
        "email": "admin@email.com",
        "password": "admin"
    }

    auth_resp = requests.post(op_addr + "/auth/login", json=auth_body, headers={"Content-Type": "application/json"})

    return auth_resp.json()['accessToken']


def test_count_events_i64():
    assert agg_prop_ch_query("count", "event_user_id") == simple_op_query("countEvents")


def test_count_events_u8():
    assert agg_prop_ch_query("count", "user_cart_items_number") == simple_op_query("countEvents")


def test_count_events_decimal():
    assert agg_prop_ch_query("count", "event_revenue") == simple_op_query("countEvents")


def test_count_unique_groups_u8():
    assert agg_prop_ch_query("count", "user_cart_items_number") == simple_op_query("countUniqueGroups")


def test_count_unique_groups_i64():
    assert agg_prop_ch_query("count", "event_user_id") == simple_op_query("countUniqueGroups")


def test_count_unique_groups_decimal():
    assert agg_prop_ch_query("count", "event_revenue") == simple_op_query("countUniqueGroups")


def test_count_uint8():
    assert_agg_prop("count", "user_cart_items_number")


def test_min_uint8():
    assert_agg_prop("min", "user_cart_items_number")


def test_max_uint8():
    assert_agg_prop("max", "user_cart_items_number")


def test_sum_uint8():
    assert_agg_prop("sum", "user_cart_items_number")


def test_avg_uint8():
    assert_agg_prop("avg", "user_cart_items_number")


def test_count_decimal():
    assert_agg_prop("count", "event_revenue")


def test_min_decimal():
    assert_agg_prop("min", "event_revenue")


def test_max_decimal():
    assert_agg_prop("max", "event_revenue")


def test_sum_decimal():
    assert_agg_prop("sum", "event_revenue")


def test_avg_decimal():
    assert_agg_prop("avg", "event_revenue")


def test_partitioned_avg_sum_uint8():
    assert_partitioned_agg_prop("avg", "sum", "event_revenue")


def test_partitioned_count():
    ch_query = """select c, avg(counts),min(counts),max(counts)
    from (
             select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, count(1) as counts
             from file('*.parquet', Parquet) as b
             where b.event_event = 'Order Completed'
               and toStartOfDay(event_created_at, 'UTC') >=
                   toStartOfDay(now(), 'UTC') - INTERVAL 2 day
             group by event_user_id, c)
    group by c
    order by 1 asc format JSONCompactColumns;"""

    ch_resp = requests.get(ch_addr,
                           params={"query": ch_query})
    ch_ts = list(map(lambda x: x * 1000000000, ch_resp.json()[0]))
    ch_val1 = list(map(lambda x: float(x), ch_resp.json()[1]))
    ch_val2 = list(map(lambda x: float(x), ch_resp.json()[2]))
    ch_val3 = list(map(lambda x: float(x), ch_resp.json()[3]))

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
                "eventName": "Order Completed",
                "queries": [
                    {
                        "type": "countPerGroup",
                        "aggregate": "avg"
                    },
                    {
                        "type": "countPerGroup",
                        "aggregate": "min"
                    },
                    {
                        "type": "countPerGroup",
                        "aggregate": "max"
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

    op_token = auth()
    op_resp = requests.post(
        "{0}/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact".format(op_addr),
        json=op_query,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + op_token})

    op_ts = op_resp.json()[1]
    op_val1 = op_resp.json()[2]
    op_val2 = op_resp.json()[3]
    op_val3 = op_resp.json()[3]

    assert ch_ts == op_ts
    assert ch_val1 == op_val1
    assert ch_val2 == op_val2
    assert ch_val3 == op_val3
