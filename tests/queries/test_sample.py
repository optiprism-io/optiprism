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
        """SELECT toUnixTimestamp(toDate(event_created_at)) , count(1), count(distinct event_user_id)
FROM file('*.parquet', Parquet)
where event_event = 'Product Viewed'
  and toStartOfDay(event_created_at, 'UTC') >
      toStartOfDay(parseDateTime('2023-09-21', '%Y-%m-%d'), 'UTC')- INTERVAL 20 day
group by 1
order by 1 desc format JSONColumns;""",
    {
        "time": {
            "type": "last",
            "last": 20,
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
op_addr = "http://localhost:8080/api/v1/organizations/1/projects/1/queries/event-segmentation"
op_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJleHAiOjE2OTQwMjQyMDIsImFjY291bnRJZCI6MX0.I0T8P-b13-0A-6c0BI2MU9JgxlrCMr7VSBfmlpc3-FE8Xjsf8LTdRGCWkVbR_T_ddwD-TEVXMUJI4YbiC3dU_w"


def test_sql_queries():
    for query in queries:
        ch_query = query.ch_query.replace("{from}", "file('*.parquet', Parquet)").replace("{format}", "JSONCompact")
        print(ch_query)
        ch_resp = requests.get(ch_addr, params={"query": ch_query})
        print(ch_resp.json())

        op_resp = requests.post(op_addr, json=query.op_query, headers={"Content-Type": "application/json",
                                                                       "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJleHAiOjE2OTQwMjQyMDIsImFjY291bnRJZCI6MX0.I0T8P-b13-0A-6c0BI2MU9JgxlrCMr7VSBfmlpc3-FE8Xjsf8LTdRGCWkVbR_T_ddwD-TEVXMUJI4YbiC3dU_w"})
        print(json.dumps(op_resp.json(), indent=2))
