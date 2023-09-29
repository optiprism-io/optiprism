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
                             breakdowns=None):
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
                "breakdowns": breakdowns,
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
    if breakdowns != None:
        group = resp.json()[2]
        val = resp.json()[3]

        return [ts, group, val]
    else:
        val = resp.json()[2]

        return [ts, val]


def partitioned_aggregate_property_query(agg, outer_agg, typ: str, prop_type="event", last=3, interval_unit="day",
                                         breakdowns=None):
    q = {
        "time": {
            "type": "last",
            "last": last,
            "unit": "day"
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
                "breakdowns": breakdowns,
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
    if breakdowns != None:
        group = resp.json()[2]
        val = resp.json()[3]

        return [ts, group, val]
    else:
        val = resp.json()[2]

        return [ts, val]


def partitioned_aggregate_property_query_grouped(agg, outer_agg, typ: str, prop_type="event", last=3,
                                                 interval_unit="day",
                                                 breakdowns=None):
    q = {
        "time": {
            "type": "last",
            "last": last,
            "unit": "day"
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
                "breakdowns": breakdowns,
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
    group = resp.json()[1]
    val = resp.json()[2]

    return [ts, group, val]
