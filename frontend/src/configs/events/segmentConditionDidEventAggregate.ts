import {
    DidEventCountTypeEnum,
    DidEventRelativeCountTypeEnum,
    DidEventAggregatePropertyTypeEnum,
} from '@/api'

export const aggregates = [
    {
        key: DidEventCountTypeEnum.Count,
    },
    {
        key: DidEventRelativeCountTypeEnum.RelativeCount,
    },
    {
        key: DidEventAggregatePropertyTypeEnum.AggregateProperty,
        hasProperty: true,
    },
    // {
    //     key: 'historicalCount',
    // }
]