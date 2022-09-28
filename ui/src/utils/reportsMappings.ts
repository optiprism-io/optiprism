import i18n from '@/utils/i18n'
import schemaService from '@/api/services/schema.service'

import { useEventsStore, Event, EventQuery, EventBreakdown } from '@/stores/eventSegmentation/events'
import { useStepsStore } from '@/stores/funnels/steps'
import { useReportsStore } from '@/stores/reports/reports'
import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'
import { useFilterGroupsStore, FilterGroup, FilterCondition } from '@/stores/reports/filters'
import { useSegmentsStore, Segment } from '@/stores/reports/segments'
import { Each } from '@/components/uikit/UiCalendar/UiCalendar'


import {
    Property,
    EventRefOneOf1EventTypeEnum,
    EventRefOneOfEventTypeEnum,
    Value,
    EventType,
    PropertyType,
    PropertyValuesListRequestPropertyTypeEnum,
    EventQueryQuery,

    QueryFormulaTypeEnum,
    QuerySimpleTypeEnum,
    QueryCountPerGroupTypeEnum,
    QueryAggregatePropertyPerGroupTypeEnum,
    QueryAggregatePropertyTypeEnum,

    EventSegmentationEvent,
    EventFiltersGroupsInner,
    EventSegmentationSegment,
    SegmentCondition,

    SegmentConditionDidEventTypeEnum,
    SegmentConditionFunnelTypeEnum,
    SegmentConditionHadPropertyValueTypeEnum,
    SegmentConditionHasPropertyValueTypeEnum,

    DidEventCountTypeEnum,
    DidEventRelativeCountTypeEnum,
    DidEventAggregatePropertyTypeEnum,
    DidEventHistoricalCountTypeEnum,

    DidEventAggregateProperty,
    DidEventHistoricalCount,
    TimeBetweenTypeEnum,
    Event as EventItem,
    EventFilterByProperty,
    PropertyValuesListRequestEventTypeEnum,

    TimeBetween,
    TimeLast,
    TimeAfterFirstUse,
    TimeWindowEach,
    TimeLastTypeEnum,
    TimeAfterFirstUseTypeEnum,
    TimeWindowEachTypeEnum,
    DidEventCountTime,
} from '@/api'

import { AggregateId } from '@/types/aggregate'
import { Step } from '@/types/steps'
import { EventRef, EventQueryRef, Condition } from '@/types/events'
import { Filter } from '@/types/filters'

type GetValues = {
    eventName?: string
    eventType?: PropertyValuesListRequestEventTypeEnum
    propertyName: string
    propertyType?: PropertyValuesListRequestPropertyTypeEnum
}

type GetTime = {
    time: TimeAfterFirstUse | TimeBetween | TimeLast | TimeWindowEach
}

const getTime = (props: GetTime) => {
    let each = null
    let period = {
        from: '',
        to: '',
        last: 0,
        type: props.time.type === 'windowEach' ? 'each' : props.time.type
    }

    switch(props.time.type) {
        case TimeLastTypeEnum.Last:
            period.last = props.time.n
        case TimeLastTypeEnum.Last:
        case TimeAfterFirstUseTypeEnum.AfterFirstUse:
        case TimeWindowEachTypeEnum.WindowEach:
            each = props.time?.unit as Each
            break;
        case TimeBetweenTypeEnum.Between:
            period.from = props.time.from
            period.to = props.time.to
            break;
    }

    return {
        each,
        period
    }
}

const getValues = async (props: GetValues) => {
    const commonStore = useCommonStore()
    let valuesList: Array<boolean> | Array<number> | Array<string> = []

    try {
        const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
            eventName: props.eventName,
            eventType: props.eventType,
            propertyName: props.propertyName,
            propertyType: props.propertyType,
        })

        if (res.data.values) {
            valuesList = res.data.values
        }
    } catch (error) {
        throw new Error('error get events values')
    }

    return valuesList
}

const computedFilter = async (eventName: string | undefined, eventType: PropertyValuesListRequestEventTypeEnum | undefined, items: EventFilterByProperty[]) => {
    return await Promise.all(items.map(async filter => {
        return {
            propRef: {
                type: filter.propertyType as PropertyType,
                id: filter.propertyId as number
            },
            opId: filter.operation,
            values: filter.value || [],
            valuesList: await getValues({
                eventName: eventName,
                eventType: eventType,
                propertyName: filter.propertyName || '',
                propertyType: filter.propertyType as PropertyValuesListRequestPropertyTypeEnum,
            }),
        }
    }))
}

const mapReportToEvents = async (items: EventSegmentationEvent[]): Promise<Event[]> => {
    const commonStore = useCommonStore()

    return await Promise.all(items.map(async (item): Promise<Event> => {
        return {
            ref: {
                type: item.eventType,
                id: item.eventId || 0,
            },
            filters: item.filters ? await computedFilter(item.eventName, item.eventType, item.filters) : [],
            queries: item.queries.map((row, i): EventQuery => {
                const query = row.query as EventQueryQuery

                const queryRef: EventQueryRef = {
                    type: query.type,
                    name: row.name,
                }

                switch(query.type) {
                    case QueryFormulaTypeEnum.Formula:
                        queryRef.value = query.formula
                        break;
                    case QuerySimpleTypeEnum.Simple:
                        break;
                    case QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup:
                        queryRef.typeGroupAggregate = query.aggregatePerGroup as AggregateId
                    case QueryAggregatePropertyTypeEnum.AggregateProperty:
                    case QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup:
                        queryRef.propRef = {
                            type: query.propertyType,
                            id: query.propertyId || 0
                        }
                    case QueryAggregatePropertyTypeEnum.AggregateProperty:
                    case QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup:
                    case QueryCountPerGroupTypeEnum.CountPerGroup:
                        queryRef.typeAggregate = query.aggregate as AggregateId
                        break
                }

                return {
                    queryRef,
                    noDelete: i === 0
                }
            }),
            breakdowns: item.breakdowns ? item.breakdowns.map((row): EventBreakdown =>  {
                return {
                    propRef: {
                        type: row.propertyType as PropertyType,
                        id: row.propertyId || 0
                    }
                }
            }) : [],
        }
    }))
}

const mapReportToFilterGroups = async (items: EventFiltersGroupsInner[]): Promise<FilterGroup[]> => {
    const commonStore = useCommonStore()

    return await Promise.all(items.map(async (item): Promise<FilterGroup> => {
        return {
            condition: item.filtersCondition as FilterCondition,
            filters: item.filters ? await Promise.all(item.filters.map(async (filter): Promise<Filter> => {
                let valuesList: string[] | boolean[] | number[] = []
                try {
                    const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
                        propertyName: filter.propertyName || '',
                        propertyType: filter.propertyType
                    })

                    if (res.data.values) {
                        valuesList = res.data.values
                    }
                } catch (error) {
                    throw new Error('error get values');
                }
                return {
                    propRef: {
                        type: filter.propertyType,
                        id: filter.propertyId || 0,
                    },
                    opId: filter.operation,
                    values: filter.value || [],
                    valuesList,
                }
            })) : []
        }
    }))
}

const mapReportToSegments = async (items: EventSegmentationSegment[]): Promise<Segment[]> => {
    const lexiconStore = useLexiconStore()

    return await Promise.all(items.map(async (item): Promise<Segment> => {
        return {
            name: item.name || '',
            conditions: await Promise.all(item.conditions.map(async (row): Promise<Condition> => {
                const condition = row

                const res: Condition = {
                    action: {
                        name: i18n.t(`events.condition.${condition.type}`),
                        id: condition.type || '',
                    },
                    filters: []
                }

                switch(condition.type) {
                    case SegmentConditionDidEventTypeEnum.DidEvent:
                        if (condition.eventName || condition.eventId) {
                            res.event = {
                                name: condition.eventName || '',
                                ref: {
                                    type: condition.eventType || EventType.Regular,
                                    id: condition.eventId || 0,
                                }
                            }
                        }

                        if (condition.filters) {
                            res.filters = await computedFilter(condition.eventName, condition.eventType, condition.filters)
                        }

                        if (condition.aggregate) {
                            res.opId = condition.aggregate.operation

                            if (condition.aggregate.type !== DidEventRelativeCountTypeEnum.DidEventRelativeCount && condition.aggregate?.value) {
                                res.valueItem = condition.aggregate.value
                            }

                            if (condition.aggregate.time) {
                                const { each, period } = getTime({ time: condition.aggregate.time });
                                res.period = period
                                res.each = each as Each
                            }

                            if (condition.aggregate.type === DidEventRelativeCountTypeEnum.DidEventRelativeCount && condition.aggregate.rightEvent) {
                                let event: EventItem | null = null

                                switch(condition.aggregate.rightEvent.eventType) {
                                    case EventRefOneOfEventTypeEnum.Regular:
                                        event = lexiconStore.findEventByName(condition.aggregate.rightEvent.eventName || '')
                                        break
                                    case EventRefOneOf1EventTypeEnum.Custom:
                                        event = lexiconStore.findCustomEventById(condition.aggregate.rightEvent.eventId || 0)
                                        break
                                }

                                if (event) {
                                    res.compareEvent = {
                                        name: event.name,
                                        ref: {
                                            type: condition.aggregate.rightEvent.eventType as EventType,
                                            id: event.id,
                                        }
                                    }
                                }
                            }
                            res.aggregate = {
                                name: i18n.t(`events.aggregates.${condition.aggregate.type}`),
                                id: condition.aggregate.type,
                            }
                        }
                        break;
                    case SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue:
                        if (condition.time) {
                            const { each, period } = getTime({ time: condition.time });
                            res.period = period
                            res.each = each as Each
                        }
                    case SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue:
                    case SegmentConditionHasPropertyValueTypeEnum.HasPropertyValue:
                        const property: Property = lexiconStore.findEventPropertyByName(condition.propertyName) || lexiconStore.findUserPropertyByName(condition.propertyName)

                        res.valuesList = await getValues({
                            propertyName: property.name,
                            propertyType: property.type as PropertyType,
                        })
                        res.opId = condition.operation
                        res.propRef = {
                            type: property.type as PropertyType,
                            id: property.id
                        }
                        res.values = condition.values
                }

                return res;
            }))
        }
    }))
}

export const funnelsToEvents = () => {
    const eventsStore = useEventsStore()
    const stepsStore = useStepsStore()

    eventsStore.events = stepsStore.steps.reduce((items: Event[], step) => {
        step.events.forEach(stepEvent => {
            items.push({
                ref: stepEvent.event,
                filters: stepEvent.filters,
                breakdowns: [],
                queries: [{
                    noDelete: true,
                    queryRef: {
                        name: 'countEvents',
                        type: 'simple'
                    }
                }]
            })
        })

        return items
    }, [])
}

export const eventsToFunnels = () => {
    const eventsStore = useEventsStore()
    const stepsStore = useStepsStore()

    stepsStore.steps = eventsStore.events.map((event): Step => {
        return {
            events: [{
                event: event.ref as EventRef,
                filters: event.filters as Filter[],
            }]
        }
    })
}

export const reportToEvents = async (id: string) => {
    const reportsStore = useReportsStore()
    const eventsStore = useEventsStore()
    const filterGroupsStore = useFilterGroupsStore()
    const segmentsStore = useSegmentsStore()

    reportsStore.reportId = id
    const report = reportsStore.activeReport?.report

    eventsStore.events = report?.events ? await mapReportToEvents(report.events) : []
    filterGroupsStore.condition = report?.filters?.groupsCondition || 'and'
    filterGroupsStore.filterGroups = report?.filters?.groups ? await mapReportToFilterGroups(report.filters.groups) : []
    segmentsStore.segments = report?.segments ? await mapReportToSegments(report.segments) : []
}

export const reportToFunnels = (id: string) => {
    // TODO
}