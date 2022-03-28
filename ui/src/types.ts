export enum DataType {
    String = 'string',
    Number = 'number',
    Boolean = 'boolean',
}

export type Value = string | number | boolean;

export interface Cohort {
    id: number;
    name: string;
}

export enum OperationId {
    Eq = "=",
    Neq = "neq",
    Gt = "gt",
    Gte = "gte",
    Lt = "lt",
    Lte = "lte",
    True = "true",
    False = "false",
    Exists = "exists",
    Empty = "empty",
    ArrAll = "arr_all",
    ArrNone = "arr_none",
    Regex = "regex"
}

export interface Operation {
    id: OperationId;
    name: string;
    shortName?: string;
    dataTypes?: DataType[];
    flags?: OpFlag[];
}

enum OpFlag {
    Null,
    Array
}

export const operations: Operation[] = [
    {
        id: OperationId.Eq,
        name: "Equal (=)",
        shortName: '='
    },
    {
        id: OperationId.Neq,
        name: "Not Equal (!=)",
        shortName: '!=',
    },
    {
        id: OperationId.Gt,
        name: "Greater (>)",
        dataTypes: [DataType.Number],
        shortName: '>',
    },
    {
        id: OperationId.Gte,
        name: "Greater or Equal (>=)",
        dataTypes: [DataType.Number],
        shortName: '>=',
    },
    {
        id: OperationId.Lt,
        name: "Less (<)",
        dataTypes: [DataType.Number],
        shortName: '<',
    },
    {
        id: OperationId.Lte,
        name: "Less or Equal (<=)",
        dataTypes: [DataType.Number],
        shortName: '<=',
    },
    {
        id: OperationId.True,
        name: "True",
        dataTypes: [DataType.Boolean],
    },
    {
        id: OperationId.False,
        name: "False",
        dataTypes: [DataType.Boolean]
    },
    {
        id: OperationId.Exists,
        name: "Exists",
        flags: [OpFlag.Null]
    },
    {
        id: OperationId.Empty,
        name: "Is Empty",
        flags: [OpFlag.Null]
    },
    {
        id: OperationId.ArrAll,
        name: "All in array",
        flags: [OpFlag.Array]
    },
    {
        id: OperationId.ArrNone,
        name: "None in array",
        flags: [OpFlag.Array]
    },
    {
        id: OperationId.Regex,
        name: "Regex",
        dataTypes: [DataType.String]
    }
];

export const operationById: Map<OperationId, Operation> = new Map();
operations.forEach(op => operationById.set(op.id, op));

export const findOperations = (
    type: DataType,
    nullable: boolean,
    isArray: boolean
): Operation[] => {
    return operations.filter(op => {

        if (!op.dataTypes && !op.flags) {
            return true
        }

        if (op.dataTypes && op.dataTypes.find(t => t === type)) {
            return true;
        }

        if (nullable && op.flags && op.flags.find(f => f === OpFlag.Null)) {
            return true;
        }

        if (isArray && op.flags && op.flags.find(f => f === OpFlag.Array)) {
            return true;
        }
    });
};

export enum Group {
    User = "user",
    Country = "country"
}

export type TimeUnit = 'day' | 'week' | 'month' | 'year'
