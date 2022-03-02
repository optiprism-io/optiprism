import { PropertyRef, EventRef } from '@/types/events'
import { OperationId, Value } from '@/types'

export type Ids = {
    idx: number
    idxParent: number
}

type FilterIds = Ids & {
    idxFilter: number
}

export interface ChangeFilterPropertyCondition extends FilterIds {
    propRef: PropertyRef
}

export interface ChangeEventCondition extends Ids {
    ref: EventRef
}

export type RemoveFilterCondition = FilterIds

export interface ChangeFilterOperation extends FilterIds {
    opId: OperationId
}

export interface FilterValueCondition extends FilterIds {
    value: Value
}