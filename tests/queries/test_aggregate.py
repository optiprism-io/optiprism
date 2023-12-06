import math
import sys

import requests

import main
from queries import clickhouse, optiprism


def test_types():
    for field in main.fields:
        for agg in main.aggs:
            print("Test Aggregate Property {0}({1})".format(agg, field))
            typ = field.replace("_", "")
            t1 = main.return_type(typ, agg)
            if t1 == "f64" or t1 == "i128" or t1 == "u128":
                op = optiprism.aggregate_property_query(agg, field)
                ch = clickhouse.aggregate_property_query(agg, field)

                for idx, v in enumerate(ch[1]):
                    assert math.isclose(op[1][idx], v, rel_tol=0.000001)
            else:
                op = optiprism.aggregate_property_query(agg, field, period=10)
                ch = clickhouse.aggregate_property_query(agg, field, period=10)
                print(ch)
                print(op)
                assert ch == op


def test_periods():
    for interval in ["minute", "hour", "day", "week", "month", "year"]:
        for period in [1, 2, 10, 20, 30, 60]:
            print("Test Period interval={interval}, period={period}, period_interval={period_interval}".format(
                interval=interval, period=period, period_interval=interval), flush=True)
            ch = clickhouse.aggregate_property_query("sum", "i_8", interval=interval, period=period,
                                                     period_interval=interval)
            op = optiprism.aggregate_property_query("sum", "i_8", period=period, time_unit=interval,
                                                    interval_unit=interval)
            print(ch)
            print(op)
            assert ch == op


def test_grouped():
    agg = "sum"
    group = "group"
    print(optiprism.token)

    op = optiprism.aggregate_property_query(agg, group, breakdowns=[group])
    ch = clickhouse.aggregate_property_query(agg, group, group=group)
    assert ch == op


def test_all_aggregates():
    for field in main.fields:
        op = optiprism.all_aggregates_query(field)
        ch = clickhouse.all_aggregates_query(field)

        assert ch == op
