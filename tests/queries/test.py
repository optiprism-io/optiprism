import math

import requests
import numpy as np

ch_addr = "http://localhost:8123"
op_addr = "http://localhost:8080/api/v1"

aggs = ["min", "max", "avg", "sum", "count"]
fields = ["i_8", "i_16", "i_32", "i_64", "u_8", "u_16", "u_32", "u_64",
          # "f_32",
          "f_64", "decimal"]


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


def assert_agg_prop_approx(agg, field: str):
    ch = agg_prop_ch_query(agg, field)
    op = agg_prop_op_query(agg, field)

    for idx, v in enumerate(ch[1]):
        print(idx,v)
        assert math.isclose(op[1][idx], v, rel_tol=0.000001)


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


def partitioned_agg_prop_op_query(agg, outer_agg, typ: str, prop_type="event"):
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
                        "propertyName": typ
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


def assert_partitioned_agg_prop(agg, outer_agg, typ: str):
    assert partitioned_agg_prop_ch_query(agg, outer_agg, typ) == partitioned_agg_prop_op_query(agg, outer_agg, typ)


def assert_partitioned_agg_prop_approx(agg, outer_agg, field: str):
    ch = partitioned_agg_prop_ch_query(agg, outer_agg, field)
    op = partitioned_agg_prop_op_query(agg, outer_agg, field)

    for idx, v in enumerate(ch[1]):
        assert math.isclose(op[1][idx], v, rel_tol=0.0000001)


def test_count_events():
    assert agg_prop_ch_query("count", "event") == simple_op_query("countEvents")


def test_count_unique_groups():
    assert agg_prop_ch_query("uniq", "user_id", "") == simple_op_query("countUniqueGroups")


def test_agg_prop():
    for field in fields:
        for agg in aggs:
            print("Test Aggregate Property {0}({1})".format(agg, field))
            typ = field.replace("_", "")
            t1 = return_type(typ, agg)
            if t1 == "f64":
                assert_agg_prop_approx(agg, field)
            else:
                assert_agg_prop(agg, field)


def test_partitioned_agg():
    for field in fields:
        for outer_agg in aggs:
            for inner_agg in aggs:
                print("Test Partitioned Aggregate Property {outer}({inner}({field}))".format(outer=outer_agg,
                                                                                             inner=inner_agg,
                                                                                             field=field))
                typ = field.replace("_", "")
                t1 = return_type(typ, inner_agg)
                t2 = return_type(t1, outer_agg)
                if t2 == "f64":
                    assert_partitioned_agg_prop_approx(inner_agg, outer_agg, field)
                else:
                    assert_partitioned_agg_prop(inner_agg, outer_agg, field)


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
    elif agg == "sum":
        if typ == "i8" or typ == "i16" or typ == "i32":
            return "i64"
        elif typ == "u8" or typ == "u16" or typ == "u32":
            return "u64"
        elif typ == "i64":
            return "i128"
        elif typ == "u64":
            return "u128"
        elif typ == "f32":
            return "f64"
        elif typ == "f64":
            return "f64"
        elif typ == "i128" or typ == "u128":
            return "i128"


def agg_combinations():
    data_types = {
        "i8": "Int8",
        "i16": "Int16",
        "i32": "Int32",
        "i64": "Int64",
        "i128": "Decimal128(DECIMAL_PRECISION,DECIMAL_SCALE)",
        "u8": "UInt8",
        "u16": "UInt16",
        "u32": "UInt32",
        "u64": "UInt64",
        "u128": "Decimal128(DECIMAL_PRECISION,DECIMAL_SCALE)",
        "f32": "Float32",
        "f64": "Float64",
    }

    arrs = {
        "i8": "Int8Array",
        "i16": "Int16Array",
        "i32": "Int32Array",
        "i64": "Int64Array",
        "i128": "Int128Array",
        "u8": "UInt8Array",
        "u16": "UInt16Array",
        "u32": "UInt32Array",
        "u64": "UInt64Array",
        "u128": "Int128Array",
        "f32": "Float32Array",
        "f64": "Float64Array",
    }

    builders = {
        "i8": "Int8Builder",
        "i16": "Int16Builder",
        "i32": "Int32Builder",
        "i64": "Int64Builder",
        "i128": "Decimal128Builder",
        "u8": "UInt8Builder",
        "u16": "UInt16Builder",
        "u32": "UInt32Builder",
        "u64": "UInt64Builder",
        "u128": "DecimalBuilder",
        "f32": "Float32Builder",
        "f64": "Float64Builder",
    }
    types = ["i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "i128"]

    # agg!(i32, Int32Array, i32, i32, Int32Builder, 1, DataType::Int8);
    agg_tpl = "agg!({typ}, {arr}, {inner_type}, {outer_type}, {outer_builder}, {multiplier}, DataType::{outer_data_type});"
    outer = """
    DataType::{data_type} => {{
                    let t1 = get_return_type(DataType::{data_type}, &inner_fn);
                    let t2 = get_return_type(t1, &outer_fn);
                    match (&t1, &t2) {{
                        {inner}
                        _=>unimplemented!()
                    }}
    }}
                    """
    tpl = """(&DataType::{data_type_1}, &DataType::{data_type_2}) => {{
                            let inner = aggregate::<{agg_type_1}>(&inner_fn);
                            let outer = aggregate::<{agg_type_2}>(&outer_fn);
                            partitioned_aggregate!(
                                {typ},
                                {agg_type_1},
                                {agg_type_2},
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }}"""
    print()
    for typ in types:
        inner = ""
        cache = {}
        for agg in aggs:
            rt1 = return_type(typ, agg)
            for agg2 in aggs:
                rt2 = return_type(rt1, agg2)
                key = "{0} - {1}".format(rt1, rt2)
                if key not in cache:
                    res = tpl.format(data_type_1=data_types[rt1], data_type_2=data_types[rt2], agg_type_1=rt1,
                                     agg_type_2=rt2,
                                     typ=typ)
                    inner += res
                    cache[key] = True
        print(outer.format(data_type=data_types[typ], inner=inner))

    for typ in types:
        cache = {}
        for agg in aggs:
            rt1 = return_type(typ, agg)
            for agg2 in aggs:
                rt2 = return_type(rt1, agg2)
                key = "{0} - {1}".format(rt1, rt2)
                if key not in cache:
                    cache[key] = True
                    mul = "1"
                    if rt2 == "f64" or rt2 == "f32":
                        mul = "1."
                    elif rt1 != "i128" and rt2 == "i128":
                        mul = "DECIMAL_MULTIPLIER"
                    elif rt2 != "i128" or (rt1 == "i128" and rt2 == "i128"):
                        mul = "1"
                    print(agg_tpl.format(typ=typ, arr=arrs[typ], inner_type=rt1, outer_type=rt2,
                                         outer_builder=builders[rt2],
                                         multiplier=mul,
                                         outer_data_type=data_types[rt2]))
