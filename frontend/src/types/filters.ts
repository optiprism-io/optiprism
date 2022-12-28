import {PropertyRef} from '@/types/events'
import {OperationId, Value} from '@/types'

export interface Filter {
    propRef?: PropertyRef;
    opId: OperationId;
    values: Value[];
    valuesList: Value[]
}