import reportsMocks from './reports/reports.json'

export default [
    {
        'id': 1,
        'name': 'One Dashaboard',
        'rows': [
            {
                'panels': [
                    {
                        'span': '6',
                        'type': 'report',
                        'reportId': '31',
                        'report': reportsMocks[0]
                    },
                    {
                        'span': '6',
                        'type': 'report',
                        'reportId': '8747',
                        'report': reportsMocks[5]
                    }
                ]
            },
            {
                'panels': [
                    {
                        'span': '4',
                        'type': 'report',
                        'reportId': '3',
                        'report': reportsMocks[2]
                    },
                    {
                        'span': '4',
                        'type': 'report',
                        'reportId': '2',
                        'report': reportsMocks[3]
                    },
                    {
                        'span': '4',
                        'type': 'report',
                        'reportId': '4',
                        'report': reportsMocks[4]
                    }
                ]
            }
        ]
    },
    {
        'id': 2,
        'name': 'Two Dashaboard',
        'rows': [
            {
                'panels': [
                    {
                        'span': '2',
                        'type': 'report',
                        'reportId': '1',
                        'report': reportsMocks[1]
                    },
                    {
                        'span': '2',
                        'type': 'report',
                        'reportId': '2',
                        'report': reportsMocks[2]
                    },
                    {
                        'span': '2',
                        'type': 'report',
                        'reportId': '3',
                        'report': reportsMocks[3]
                    },
                    {
                        'span': '2',
                        'type': 'report',
                        'reportId': '4',
                        'report': reportsMocks[4]
                    }
                ]

            }
        ]
    }
]