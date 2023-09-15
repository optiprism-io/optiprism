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


def auth():
    auth_body = {
        "email": "admin@email.com",
        "password": "admin"
    }

    auth_resp = requests.post(op_addr + "/auth/login", json=auth_body, headers={"Content-Type": "application/json"})

    return auth_resp.json()['accessToken']


def test_sql_queries():
    ch_query = """SELECT toUnixTimestamp(toDate(event_created_at, 'UTC')),
       count(event_user_id)                 as q1,
       count(distinct event_user_id)        as q2
FROM file('*.parquet', Parquet)
where event_event = 'Product Viewed'
  and toStartOfDay(event_created_at, 'UTC') >=
      toStartOfDay(parseDateTime('2023-09-15', '%Y-%m-%d'), 'UTC') - INTERVAL 1 day
group by 1
order by 1 asc format JSONCompactColumns;"""

    ch_resp = requests.get(ch_addr,
                           params={"query": ch_query})
    ch_ts = list(map(lambda x: x * 1000000000,ch_resp.json()[0]))
    ch_val1 = list(map(lambda x: float(x),ch_resp.json()[1]))
    ch_val2 = list(map(lambda x: float(x),ch_resp.json()[2]))

    ch_query = """select c, avg(counts)
from (
         select toUnixTimestamp(toDate(event_created_at, 'UTC')) as c, count(1) as counts
         from file('*.parquet', Parquet) as b
         where b.event_event = 'Product Viewed'
           and toStartOfDay(event_created_at, 'UTC') >=
               toStartOfDay(parseDateTime('2023-09-15', '%Y-%m-%d'), 'UTC') - INTERVAL 1 day
         group by event_user_id, c)
group by c
order by 1 asc format JSONCompactColumns;"""

    ch_resp = requests.get(ch_addr,
                           params={"query": ch_query})
    ch_val3 = list(map(lambda x: float(x), ch_resp.json()[1]))

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
                "eventName": "Product Viewed",
                "queries": [
                    {
                        "type": "countEvents"
                    },
                    {
                        "type": "countUniqueGroups"
                    },
                    {
                        "type": "countPerGroup",
                        "aggregate": "avg"
                    }
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

    print("op resp")
    op_ts = op_resp.json()[1]
    op_val1 = op_resp.json()[2]
    op_val2 = op_resp.json()[3]
    op_val3 = op_resp.json()[4]

    assert ch_ts == op_ts
    assert ch_val1 == op_val1
    assert ch_val2 == op_val2
    assert ch_val3 == op_val3
