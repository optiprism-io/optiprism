import {
    DidEventCountTypeEnum,
    DidEventRelativeCountTypeEnum,
    DidEventAggregatePropertyTypeEnum,
} from '@/api'

export const aggregates = [
    {
        key: DidEventCountTypeEnum.DidEventCount,
    },
    {
        key: DidEventRelativeCountTypeEnum.DidEventRelativeCount,
    },
    {
        key: DidEventAggregatePropertyTypeEnum.AggregateProperty,
        hasProperty: true,
    },
    // {
    //     key: 'historicalCount',
    // }
]