import math

import requests

import main
from queries import clickhouse, optiprism


def test_aggregate_property():
    for field in main.fields:
        for agg in main.aggs:
            print("Test Aggregate Property {0}({1})".format(agg, field))
            typ = field.replace("_", "")
            t1 = main.return_type(typ, agg)
            if t1 == "f64" or t1 == "i128" or t1 == "u128":
                ch = clickhouse.aggregate_property_query(agg, field)
                op = optiprism.aggregate_property_query(agg, field)

                for idx, v in enumerate(ch[1]):
                    assert math.isclose(op[1][idx], v, rel_tol=0.000001)
            else:
                ch = clickhouse.aggregate_property_query(agg, field)
                op = optiprism.aggregate_property_query(agg, field)
                assert ch == op


def test_aggregate_property_grouped():
    agg = "sum"
    group = "group"

    ch = clickhouse.aggregate_property_query_grouped(agg, group, group=group)
    breakdowns = [
        {
            "type": "property",
            "propertyType": "event",
            "propertyName": group
        }
    ]
    op = optiprism.aggregate_property_query(agg, group, breakdowns=breakdowns)
    assert ch == op
