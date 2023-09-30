import math

import requests
import numpy as np




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
