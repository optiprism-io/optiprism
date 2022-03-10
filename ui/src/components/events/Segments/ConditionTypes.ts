import { PropertyRef, EventRef } from '@/types/events'
import { OperationId, Value } from '@/types'
import { ApplyPayload } from '@/components/uikit/UiDatePicker.vue'

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

export interface PeriodConditionPayload extends Ids {
    value: ApplyPayload
}

export interface PayloadChangeAgregateCondition extends Ids {
    value: {
        id: string
        name: string
    }
}