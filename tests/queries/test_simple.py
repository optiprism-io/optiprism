from queries import optiprism, clickhouse


def test_count_events():
    ch = clickhouse.aggregate_property_query("count", "i_8", period=3)
    op = optiprism.simple_query("countEvents", time_last=3)

    assert ch == op


def test_count_events_grouped():
    ch = clickhouse.aggregate_property_query("count", "i_8", group="group")
    op = optiprism.simple_query("countEvents", breakdowns=["group"])

    assert ch == op


# fixme
def test_count_events_uniq():
    op = optiprism.simple_query("countUniqueGroups", time_last=3)
    ch = clickhouse.aggregate_property_query("uniq", "user_id", period=3)

    print(ch)
    print(op)
    assert ch == op
