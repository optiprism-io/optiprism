import { defineStore } from "pinia";
import {
    Condition,
    PropertyRef,
    ConditionFilter,
    PropertyType,
} from "@/types/events";
import { OperationId, Value } from "@/types";
import schemaService from "@/api/services/schema.service";
import { useLexiconStore } from "@/stores/lexicon";
import {
    ChangeFilterPropertyCondition,
    ChangeEventCondition,
    RemoveFilterCondition,
    ChangeFilterOperation,
    FilterValueCondition,
    Ids,
    PeriodConditionPayload,
    PayloadChangeAgregateCondition,
    PayloadChangeValueItem,
    PayloadChangeEach,
} from '@/components/events/Segments/ConditionTypes'
import { getYYYYMMDD } from '@/helpers/getStringDates'
import { getLastNDaysRange } from '@/helpers/calendarHelper'

interface Segment {
    name: string
    conditions?: Condition[]
}

type SegmentsStore = {
    segments: Segment[]
}

export const useSegmentsStore = defineStore("segments", {
    state: (): SegmentsStore => ({
        segments: [],
    }),
    getters: {},
    actions: {
        inputCalendarEach(payload: PayloadChangeEach) {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]
                if (condition) {
                    condition.each = payload.value
                    condition.period = {
                        type: 'each',
                    }
                }
            }
        },
        inputValue(payload: PayloadChangeValueItem) {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]
                if (condition) {
                    condition.valueItem = payload.value
                }
            }
        },
        changeAgregateCondition(payload: PayloadChangeAgregateCondition) {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]
                if (condition) {
                    delete condition.propRef
                    condition.aggregate = payload.value
                    condition.opId = OperationId.Gte
                    condition.period = {
                        type: 'each',
                    }
                    condition.each = 'day'
                    condition.filters = []
                    condition.values = []
                    condition.valueItem = 1
                }
            }
        },
        removeFilterValueCondition(payload: FilterValueCondition) {
            const segment = this.segments[payload.idxParent]
            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition) {
                    condition.filters[payload.idxFilter].values =
                    condition.filters[payload.idxFilter].values.filter(v =>  v !== payload.value)
                }
            }
        },
        addFilterValueCondition(payload: FilterValueCondition) {
            const segment = this.segments[payload.idxParent]
            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition) {
                    condition.filters[payload.idxFilter].values.push(payload.value)
                }
            }
        },
        changeFilterOperation(payload: ChangeFilterOperation) {
            const segment = this.segments[payload.idxParent]
            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition) {
                    condition.filters[payload.idxFilter].opId = payload.opId
                    condition.filters[payload.idxFilter].values = []
                }
            }
        },
        changeEventCondition(payload: ChangeEventCondition) {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition) {
                    const lexiconStore = useLexiconStore()
                    const event = lexiconStore.findEvent(payload.ref);

                    condition.event = {
                        name: event?.displayName || event.name,
                        ref: payload.ref,
                    }
                    condition.filters = []
                }
            }
        },
        changeCompareEventCondition(payload: ChangeEventCondition) {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition) {
                    const lexiconStore = useLexiconStore()
                    const event = lexiconStore.findEvent(payload.ref);

                    condition.compareEvent = {
                        name: event?.displayName || event.name,
                        ref: payload.ref,
                    }
                }
            }
        },
        async changeFilterPropertyCondition(payload: ChangeFilterPropertyCondition) {
            const segment = this.segments[payload.idxParent]
            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition && condition.event) {
                    const eventRef = condition.event.ref;
                    let valuesList: string[] = []

                    try {
                        const lexiconStore = useLexiconStore()

                        const res = await schemaService.propertiesValues({
                            event_name: lexiconStore.eventName(eventRef),
                            event_type: eventRef.type,
                            property_name: lexiconStore.propertyName(payload.propRef),
                            property_type: PropertyType[payload.propRef.type]
                        })

                        if (res) {
                            valuesList = res
                        }
                    } catch (error) {
                        throw new Error('error getEventsValues')
                    }

                    condition.filters[payload.idxFilter] = {
                        propRef: payload.propRef,
                        opId: OperationId.Eq,
                        values: [],
                        valuesList: valuesList
                    }
                }
            }
        },
        removeFilterCondition(payload: RemoveFilterCondition): void {
            const segment = this.segments[payload.idxParent]
            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition) {
                    condition.filters.splice(payload.idxFilter, 1);
                }
            }
        },
        addFilterCondition(payload: Ids): void {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]
                const emptyFilter = condition.filters.find((filter): boolean => filter.propRef === undefined)

                if (emptyFilter) {
                    return
                }

                if (condition && condition.filters) {
                    condition.filters.push(<ConditionFilter>{
                        opId: OperationId.Eq,
                        values: [],
                        valuesList: []
                    })
                }
            }
        },
        changePeriodCondition(payload: PeriodConditionPayload): void {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                const condition = segment.conditions[payload.idx]

                if (condition && condition.period) {
                    condition.period = {
                        from: payload.value.value.from || '',
                        to: payload.value.value.to || '',
                        last: payload.value.last,
                        type: payload.value.type,
                    }
                    delete condition.each
                }
            }
        },
        removeValueCondition(idx: number, idxSegment: number, value: Value): void {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                const condition = segment.conditions[idx]
                if (condition && condition.values) {
                    condition.values = condition.values.filter(v => v !== value)
                }
            }
        },
        addValueCondition(idx: number, idxSegment: number, value: Value): void {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                const condition = segment.conditions[idx]
                if (condition && condition.values) {
                    condition.values.push(value)
                } else {
                    condition.values = [value]
                }
            }
        },
        changeOperationCondition(idx: number, idxSegment: number, opId: OperationId): void {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                const condition = segment.conditions[idx]
                if (condition) {
                    condition.opId = opId
                    condition.values = []
                }
            }
        },
        async changePropertyCondition(idx: number, idxSegment: number, ref: PropertyRef) {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                const condition = segment.conditions[idx]

                if (condition) {
                    const lexiconStore = useLexiconStore()

                    try {
                        const res = await schemaService.propertiesValues({
                            // TODO integration with backand
                            // check condition type
                            property_name: lexiconStore.propertyName(ref),
                        })

                        if (res) {
                            condition.valuesList = res
                        }
                    } catch (error) {
                        throw new Error('error getEventsValues')
                    }
                    condition.propRef = ref
                    condition.opId = OperationId.Eq
                    condition.values = []
                    condition.period = {
                        type: 'each',
                    }
                    condition.each = 'day'
                }
            }
        },
        changeActionCondition(idx: number, idxSegment: number, ref: {id: string, name: string}) {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                const condition = segment.conditions[idx]
                if (condition) {
                    delete condition.propRef
                    condition.action = ref
                    condition.period = {}
                    condition.filters = []
                }
            }
        },
        removeCondition(payload: Ids) {
            const segment = this.segments[payload.idxParent]

            if (segment && segment.conditions) {
                segment.conditions.splice(payload.idx, 1);

                if (!segment.conditions.length) {
                    delete segment.conditions
                }
            }
        },
        addConditionSegment(idx: number) {
            const segment = this.segments[idx];

            if (segment.conditions) {
                const length = segment.conditions.length - 1;
                if (segment.conditions[length] && segment.conditions[length].action) {
                    segment.conditions.push({
                        filters: []
                    })
                }
            } else {
                segment.conditions = [{
                    filters: []
                }]
            }
        },
        renameSegment(name: string, idx: number) {
            const segment = this.segments[idx];
            if (segment) {
                segment.name = name
            }
        },
        deleteSegment(idx: number) {
            this.segments.splice(idx, 1);
        },
        addSegment(name: string) {
            this.segments.push({name})
        },
    }
});