import math

from queries import clickhouse, optiprism, main


def test_types():
    for field in main.fields:
        for outer_agg in main.aggs:
            for inner_agg in main.aggs:
                print("Test Partitioned Aggregate Property {outer}({inner}({field}))".format(outer=outer_agg,
                                                                                             inner=inner_agg,
                                                                                             field=field))
                typ = field.replace("_", "")
                t1 = main.return_type(typ, inner_agg)
                t2 = main.return_type(t1, outer_agg)
                if t2 == "f64" or t2 == "i128" or t2 == "u128":
                    ch = clickhouse.partitioned_aggregate_property_query(inner_agg, outer_agg, field)
                    op = optiprism.partitioned_aggregate_property_query(inner_agg, outer_agg, field)

                    for idx, v in enumerate(ch[1]):
                        assert math.isclose(op[1][idx], v, rel_tol=0.0000001)

                else:
                    ch = clickhouse.partitioned_aggregate_property_query(inner_agg, outer_agg, field)
                    op = optiprism.partitioned_aggregate_property_query(inner_agg, outer_agg, field)
                    assert ch == op


def test_periods():
    for interval in ["minute", "hour", "day", "week", "month", "year"]:
        for period in [1, 2, 10, 20, 30, 60]:
            print("Test Period interval={interval}, period={period}, period_interval={period_interval}".format(
                interval=interval, period=period, period_interval=interval), flush=True)
            ch = clickhouse.partitioned_aggregate_property_query("min", "max", "i_8", interval=interval, period=period,
                                                                 period_interval=interval)
            op = optiprism.partitioned_aggregate_property_query("min", "max", "i_8", period=period, time_unit=interval,
                                                                interval_unit=interval)
            print(ch)
            print(op)
            assert ch == op
