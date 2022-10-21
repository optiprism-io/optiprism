import {defineStore} from 'pinia';
import {Condition, ConditionFilter, PropertyRef,} from '@/types/events';
import {OperationId, Value} from '@/types';
import schemaService from '@/api/services/schema.service';
import { useLexiconStore } from '@/stores/lexicon';
import { useCommonStore } from '@/stores/common'
import {
    ChangeEventCondition,
    ChangeFilterOperation,
    ChangeFilterPropertyCondition,
    FilterValueCondition,
    Ids,
    PayloadChangeAgregateCondition,
    PayloadChangeEach,
    PayloadChangeValueItem,
    PeriodConditionPayload,
    RemoveFilterCondition,
} from '@/components/events/Segments/Segments'
import {
    Event,
    EventType,
    Property,
    PropertyTypeEnum,
    PropertyFilterOperation,

    EventSegmentationSegment,
    SegmentCondition,
    SegmentConditionHasPropertyValue,
    SegmentConditionHasPropertyValueTypeEnum,
    SegmentConditionDidEvent,
    SegmentConditionDidEventTypeEnum,
    SegmentConditionDidEventEventTypeEnum,
    SegmentConditionHadPropertyValue,
    SegmentConditionHadPropertyValueTypeEnum,
    SegmentConditionHadPropertyValueTime,

    TimeBetweenTypeEnum,
    TimeWindowEachTypeEnum,
    TimeUnit,
    TimeLastTypeEnum,

    DidEventCount,
    DidEventRelativeCount,
    DidEventAggregateProperty,
    DidEventHistoricalCount,

    DidEventCountTypeEnum,
    DidEventRelativeCountTypeEnum,

    EventRefOneOf1EventTypeEnum,
    EventRefOneOfEventTypeEnum,
    DidEventAggregatePropertyTypeEnum,
    DidEventAggregatePropertyPropertyTypeEnum,
    QueryAggregatePropertyTypeEnum,
    QueryAggregate,
    EventFilterByProperty,
    EventFilterByPropertyTypeEnum,
    PropertyValuesList200ResponseValues,
    PropertyValuesListRequestEventTypeEnum,
} from '@/api'

export interface Segment {
    name: string
    conditions?: Condition[]
}

type SegmentsStore = {
    segments: Segment[]
}

const computedValueTime = (item: Condition): SegmentConditionHadPropertyValueTime => {
    if (item.period?.type === TimeBetweenTypeEnum.Between) {
        return {
            type: TimeBetweenTypeEnum.Between,
            from: String(item.period.from),
            to: String(item.period.to)
        }
    }

    if (item.period?.type === TimeLastTypeEnum.Last) {
        return {
            type: TimeLastTypeEnum.Last,
            n: Number(item.period.last),
            unit: 'day'
        }
    }

    return {
        type: TimeWindowEachTypeEnum.WindowEach,
        unit: item.each as TimeUnit
    }
}

const computedValueAggregate = (item: Condition): DidEventCount | DidEventRelativeCount | DidEventAggregateProperty | DidEventHistoricalCount => {
    const lexiconStore = useLexiconStore()
    const time = computedValueTime(item)
    const operation = item.opId as PropertyFilterOperation

    if (item.aggregate?.id === DidEventAggregatePropertyTypeEnum.AggregateProperty && item.propRef) {
        const property: Property = item.propRef.type === PropertyTypeEnum.Event ? lexiconStore.findEventPropertyById(item.propRef.id) : lexiconStore.findUserPropertyById(item.propRef.id)

        return {
            type: DidEventAggregatePropertyTypeEnum.AggregateProperty,
            time,
            operation,
            value: Number(item.valueItem),
            propertyName: property.name,
            propertyType: property.type as DidEventAggregatePropertyPropertyTypeEnum,
            propertyId: property.id,
            aggregate: {
                type: QueryAggregatePropertyTypeEnum.AggregateProperty,
                propertyName: property.name,
                propertyType: property.type as DidEventAggregatePropertyPropertyTypeEnum,
                propertyId: property.id,
                aggregate: item.aggregate.typeAggregate as QueryAggregate,
            }
        }
    }

    if (item.aggregate?.id === DidEventRelativeCountTypeEnum.DidEventRelativeCount && item.compareEvent) {
        const eventItem: Event = item.compareEvent.ref.type === EventType.Regular ? lexiconStore.findEventById(item.compareEvent.ref.id) : lexiconStore.findCustomEventById(item.compareEvent.ref.id)

        return {
            type: DidEventRelativeCountTypeEnum.DidEventRelativeCount,
            operation,
            time,
            rightEvent: {
                eventId: eventItem.id,
                eventName: eventItem.name,
                eventType: item.compareEvent.ref.type === EventRefOneOfEventTypeEnum.Regular ? EventRefOneOf1EventTypeEnum.Custom : EventRefOneOfEventTypeEnum.Regular,
            },
        }
    }

    return {
        type: DidEventCountTypeEnum.DidEventCount,
        value: Number(item.valueItem),
        operation,
        time,
    }
}

export const useSegmentsStore = defineStore('segments', {
    state: (): SegmentsStore => ({
        segments: [],
    }),
    getters: {
        segmentationItems(): EventSegmentationSegment[] {
            const lexiconStore = useLexiconStore()

            return this.segments.map(segment => {
                return {
                    name: segment.name,
                    conditions: segment.conditions ? segment.conditions.reduce((items: SegmentCondition[], item) => {
                        if (item.action?.id === SegmentConditionDidEventTypeEnum.DidEvent && item.event) {
                            if (item?.aggregate?.id === DidEventRelativeCountTypeEnum.DidEventRelativeCount && !item.compareEvent) {
                                return items
                            }

                            if (item?.aggregate?.id === DidEventAggregatePropertyTypeEnum.AggregateProperty && !item.propRef) {
                                return items
                            }

                            const eventItem: Event = item.event.ref.type === EventType.Regular ? lexiconStore.findEventById(item.event.ref.id) : lexiconStore.findCustomEventById(item.event.ref.id)

                            const condition: SegmentConditionDidEvent = {
                                type: SegmentConditionDidEventTypeEnum.DidEvent,
                                eventName: eventItem.name,
                                eventId: eventItem.id,
                                eventType: item.event.ref.type === EventType.Custom ? SegmentConditionDidEventEventTypeEnum.Custom : undefined,
                                filters: item.filters.filter(item => item.propRef).reduce((items: EventFilterByProperty[], filterRef) => {
                                    if (filterRef.propRef) {
                                        const property: Property = filterRef?.propRef.type === PropertyTypeEnum.Event ? lexiconStore.findEventPropertyById(filterRef.propRef.id) : lexiconStore.findUserPropertyById(filterRef.propRef.id)

                                        items.push({
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            propertyName: property.name,
                                            propertyType: property.type as PropertyTypeEnum,
                                            propertyId: filterRef.propRef.id,
                                            operation: filterRef.opId as PropertyFilterOperation,
                                            value: filterRef.values,
                                        })
                                    }

                                    return items
                                }, []),
                                aggregate: computedValueAggregate(item),
                            }

                            items.push(condition)
                        }

                        if (item.propRef && item.action?.id) {
                            const property: Property = item.propRef.type === PropertyTypeEnum.Event ? lexiconStore.findEventPropertyById(item.propRef.id) : lexiconStore.findUserPropertyById(item.propRef.id)

                            if (property) {
                                if (item.action?.id === SegmentConditionHasPropertyValueTypeEnum.HasPropertyValue) {
                                    const condition: SegmentConditionHasPropertyValue = {
                                        type: SegmentConditionHasPropertyValueTypeEnum.HasPropertyValue,
                                        propertyName: property.name,
                                        operation: item.opId as PropertyFilterOperation,
                                        values: item.values,
                                    }

                                    items.push(condition)
                                }


                                if (item.action?.id === SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue) {
                                    const condition: SegmentConditionHadPropertyValue = {
                                        type: SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue,
                                        propertyName: property.name,
                                        operation: item.opId as PropertyFilterOperation,
                                        values: item.values,
                                        time: computedValueTime(item),
                                    }

                                    items.push(condition)
                                }
                            }
                        }

                        return items
                    }, []) : [],
                }
            })
        },
    },
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
                    const event = lexiconStore.findEvent(payload.ref)

                    condition.event = {
                        name: 'displayName' in event ? event?.displayName || event.name : event.name,
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
                    const event = lexiconStore.findEvent(payload.ref)

                    condition.compareEvent = {
                        name: 'displayName' in event ? event?.displayName || event.name : event.name,
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
                    let valuesList: PropertyValuesList200ResponseValues = []

                    try {
                        const lexiconStore = useLexiconStore()
                        const commonStore = useCommonStore()

                        const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
                            eventName: lexiconStore.eventName(eventRef),
                            eventType: eventRef.type as PropertyValuesListRequestEventTypeEnum,
                            propertyName: lexiconStore.propertyName(payload.propRef),
                            propertyType: payload.propRef.type
                        })

                        if (res.data.values) {
                            valuesList = res.data.values
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
                    const commonStore = useCommonStore()

                    try {
                        const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
                            // TODO integration with backand
                            // check condition type
                            propertyName: lexiconStore.propertyName(ref),
                        })

                        if (res.data.values) {
                            condition.valuesList = res.data.values
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