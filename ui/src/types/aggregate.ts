export enum AggregateId {
    Sum = "sum",
    Avg = "avg",
    Median = "median",
    Min = "min",
    Max = "max",
    DistinctCount = "distinctCount",
    PercentileTh25 = "25thPercentile",
    PercentileTh75 = "75thPercentile",
    PercentileTh90 = "90thPercentile",
    PercentileTh99 = "99thPercentile"
}

export interface Aggregate {
    id: AggregateId;
    name: string;
    description?: string;
}

export const aggregates: Aggregate[] = [
    {
        id: AggregateId.Sum,
        name: "Sum"
    },
    {
        id: AggregateId.Avg,
        name: "Average"
    },
    {
        id: AggregateId.Median,
        name: "Median"
    },
    {
        id: AggregateId.Min,
        name: "Minimum"
    },
    {
        id: AggregateId.Max,
        name: "Maximum"
    },
    {
        id: AggregateId.DistinctCount,
        name: "Distinct count"
    },
    {
        id: AggregateId.PercentileTh25,
        name: "25th Percentile"
    },
    {
        id: AggregateId.PercentileTh75,
        name: "75th Percentile"
    },
    {
        id: AggregateId.PercentileTh90,
        name: "90th Percentile"
    },
    {
        id: AggregateId.PercentileTh99,
        name: "99th Percentile"
    },
]

export type AggregateRef = {
    typeAggregate?: AggregateId;
}