import json

import pytest
import requests
from attr import dataclass


@dataclass
class Query:
    ch_query: str
    op_query: object


queries = [
    Query(
        """SELECT toUnixTimestamp(toDate(event_created_at, 'UTC')) , count(1)
FROM file('*.parquet', Parquet)
where event_event = 'Product Viewed'
  and toStartOfDay(event_created_at, 'UTC') >=
      toStartOfDay(parseDateTime('2023-09-12', '%Y-%m-%d'), 'UTC')- INTERVAL 0 day
group by 1
order by 1 asc format JSONCompactColumns;""",
        {
            "time": {
                "type": "last",
                "last": 2,
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
        })
]

ch_addr = "http://localhost:8123"
op_addr = "http://localhost:8080/api/v1/organizations/1/projects/1/queries/event-segmentation?format=jsonCompact"
op_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJleHAiOjE2OTQ2MDIyNjYsImFjY291bnRJZCI6MX0.vSxWq76yvScXzMIBnGQsthYEYaHBxbG68r7FqYdU83YNDNnknQ46SbLAB0bQgLH6RXK68VdReG2PEPbgWf_URg"


def test_sql_queries():
    for query in queries:
        ch_query = query.ch_query.replace("{from}", "file('*.parquet', Parquet)").replace("{format}", "JSONCompact")
        ch_resp = requests.get(ch_addr, params={"query": ch_query})
        ch_ts = ch_resp.json()[0]
        ch_val = ch_resp.json()[1]
        ch_ts = list(map(lambda x: x * 1000000000, ch_ts))
        ch_val = list(map(lambda x: float(x), ch_val))
        print()
        print(ch_ts)
        print(ch_val)
        op_resp = requests.post(op_addr, json=query.op_query, headers={"Content-Type": "application/json",
                                                                       "Authorization": "Bearer " + op_token})

        # print(op_resp.json())
        op_ts = op_resp.json()[1]
        op_val = op_resp.json()[3]
        print()
        print(op_ts)
        print(op_val)

        assert ch_ts == op_ts
        assert ch_val == op_val
