export enum DataTypeKind {
    String,
    Number,
    Boolean
}

export enum DataType {
    String,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Boolean
}

export type Value = string | number | boolean;

export const dataTypeKinds: Map<DataType, DataTypeKind> = new Map([
    [DataType.String, DataTypeKind.String],
    [DataType.Float64, DataTypeKind.Number],
    [DataType.Int8, DataTypeKind.Number],
    [DataType.Int16, DataTypeKind.Number],
    [DataType.Int32, DataTypeKind.Number],
    [DataType.Int64, DataTypeKind.Number],
    [DataType.UInt8, DataTypeKind.Number],
    [DataType.UInt16, DataTypeKind.Number],
    [DataType.UInt32, DataTypeKind.Number],
    [DataType.UInt64, DataTypeKind.Number],
    [DataType.Boolean, DataTypeKind.Boolean]
]);

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
    typeKinds?: DataTypeKind[];
    flags?: OpFlag[];
}

enum OpFlag {
    Null,
    Array
}

export const operations: Operation[] = [
    {
        id: OperationId.Eq,
        name: "Equal (=)"
    },
    {
        id: OperationId.Neq,
        name: "Not Equal (!=)"
    },
    {
        id: OperationId.Gt,
        name: "Greater (>)",
        typeKinds: [DataTypeKind.Number]
    },
    {
        id: OperationId.Gte,
        name: "Greater or Equal (>=)",
        typeKinds: [DataTypeKind.Number]
    },
    {
        id: OperationId.Lt,
        name: "Less (<)",
        typeKinds: [DataTypeKind.Number]
    },
    {
        id: OperationId.Lte,
        name: "Less or Equal (<=)",
        typeKinds: [DataTypeKind.Number]
    },
    {
        id: OperationId.True,
        name: "True",
        typeKinds: [DataTypeKind.Boolean]
    },
    {
        id: OperationId.False,
        name: "False",
        typeKinds: [DataTypeKind.Boolean]
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
        typeKinds: [DataTypeKind.String]
    }
];

export const operationById: Map<OperationId, Operation> = new Map();
operations.forEach(op => operationById.set(op.id, op));

export const findOperations = (
    type: DataType,
    nullable: boolean,
    isArray: boolean
): Operation[] => {
    const kind = dataTypeKinds.get(type);
    return operations.filter(op => {
        if (op.typeKinds && !op.typeKinds.find(t => t === kind)) {
            return false;
        }

        if (nullable && op.flags && !op.flags.find(f => f === OpFlag.Null)) {
            return false;
        }

        if (isArray && op.flags && !op.flags.find(f => f === OpFlag.Array)) {
            return false;
        }

        return true;
    });
};

export enum Group {
    User = "user",
    Country = "country"
}

export type TimeUnit = 'day' | 'week' | 'month' | 'year'
