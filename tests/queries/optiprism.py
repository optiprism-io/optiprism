import requests

op_addr = "http://localhost:8080/api/v1"


def auth():
    auth_body = {
        "email": "admin@email.com",
        "password": "admin"
    }

    auth_resp = requests.post(op_addr + "/auth/login", json=auth_body, headers={"Content-Type": "application/json"})

    return auth_resp.json()['accessToken']


token = auth()


def aggregate_property_query(agg, field: str, prop_type="event",
                             breakdowns=None, period=2, time_unit="day", interval_unit="day"):
    b = []
    if breakdowns is not None:
        for breakdown in breakdowns:
            b.append({
                "type": "property",
                "propertyType": "event",
                "propertyName": breakdown
            })
    q = {
        "time": {
            "type": "last",
            "last": period,
            "unit": time_unit
        },
        "group": "user",
        "intervalUnit": interval_unit,
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
                "breakdowns": b,
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

    print(q)
    resp = requests.post(
        "{0}/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact".format(op_addr),
        json=q,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + token})

    if len(resp.json()[0]) == 0:
        return []

    ts = resp.json()[1]
    if breakdowns is not None:
        group = resp.json()[2]
        val = resp.json()[3]

        return [ts, group, val]
    else:
        val = resp.json()[2]

        return [ts, val]


def partitioned_aggregate_property_query(agg, outer_agg, typ: str, prop_type="event", period=2, time_unit="day",
                                         interval_unit="day",
                                         breakdowns=None):
    b = []
    if breakdowns is not None:
        for breakdown in breakdowns:
            b.append({
                "type": "property",
                "propertyType": "event",
                "propertyName": breakdown
            })

    q = {
        "time": {
            "type": "last",
            "last": period,
            "unit": time_unit
        },
        "group": "user",
        "intervalUnit": interval_unit,
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
                "breakdowns": b,
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

    if len(resp.json()[0]) == 0:
        return []

    ts = resp.json()[1]
    if breakdowns is not None:
        group = resp.json()[2]
        val = resp.json()[3]

        return [ts, group, val]
    else:
        val = resp.json()[2]

        return [ts, val]


def simple_query(query: str, time_last=2, unit="day",
                 interval_unit="day",
                 breakdowns=None):
    b = []
    if breakdowns is not None:
        for breakdown in breakdowns:
            b.append({
                "type": "property",
                "propertyType": "event",
                "propertyName": breakdown
            })

    q = {
        "time": {
            "type": "last",
            "last": time_last,
            "unit": unit
        },
        "group": "user",
        "intervalUnit": interval_unit,
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
                "filters": [],
                "breakdowns": b,
            }
        ],
        "filters": {
            "groupsCondition": "and",
            "groups": []
        },
        "segments": [],
        "breakdowns": []
    }

    print(q)
    print(token)

    resp = requests.post(
        "{0}/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact".format(op_addr),
        json=q,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + token})

    ts = resp.json()[1]
    if breakdowns is not None:
        group = resp.json()[2]
        val = resp.json()[3]

        return [ts, group, val]
    else:
        val = resp.json()[2]

        return [ts, val]
