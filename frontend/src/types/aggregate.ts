import { QueryAggregate as AggregateId } from '@/api'

export interface Aggregate {
    id: AggregateId;
    name: string;
    description?: string;
}

export const aggregates: Aggregate[] = [
    {
        id: AggregateId.Sum,
        name: 'Sum'
    },
    {
        id: AggregateId.Avg,
        name: 'Average'
    },
    {
        id: AggregateId.Median,
        name: 'Median'
    },
    {
        id: AggregateId.Min,
        name: 'Minimum'
    },
    {
        id: AggregateId.Max,
        name: 'Maximum'
    },
    {
        id: AggregateId.DistinctCount,
        name: 'Distinct count'
    },
    {
        id: AggregateId.Percentile25,
        name: '25th Percentile'
    },
    {
        id: AggregateId.Percentile75,
        name: '75th Percentile'
    },
    {
        id: AggregateId.Percentile90,
        name: '90th Percentile'
    },
    {
        id: AggregateId.Percentile99,
        name: '99th Percentile'
    },
]

export type AggregateRef = {
    typeAggregate?: AggregateId
}