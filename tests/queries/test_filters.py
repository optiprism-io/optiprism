from queries import clickhouse, optiprism


def test_like_filter_positive():
    ch = clickhouse.string_filter_query("string like 'прив%'")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "string",
        "operation": "like",
        "value": ["прив%", "пр%"]
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op


def test_like_filter_negative():
    ch = clickhouse.string_filter_query("string like 'лала%'")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "string",
        "operation": "like",
        "value": ["лала%"]
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op


def test_not_like_filter_positive():
    ch = clickhouse.string_filter_query("string not like 'прив%'")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "string",
        "operation": "notLike",
        "value": ["прив%", "пр%"]
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op


def test_regex_filter():
    ch = clickhouse.string_filter_query("match(string,'при.ет')")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "string",
        "operation": "regex",
        "value": ["при.ет"]
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op


def test_is_null_filter():
    ch = clickhouse.bool_filter_query("isNull(bool_nullable)")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "bool_nullable",
        "operation": "empty"
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op


def test_is_not_null_filter():
    ch = clickhouse.bool_filter_query("isNotNull(bool_nullable)")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "bool_nullable",
        "operation": "exists"
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op


def test_true_filter():
    ch = clickhouse.string_filter_query("bool_nullable=true")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "bool_nullable",
        "operation": "true"
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op


def test_false_filter():
    ch = clickhouse.string_filter_query("bool_nullable=false")
    filters = [{
        "type": "property",
        "propertyType": "system",
        "propertyName": "bool_nullable",
        "operation": "false"
    }]
    op = optiprism.simple_query("countEvents", filters=filters)

    assert ch == op
