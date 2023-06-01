import i18n from '@/utils/i18n'
import schemaService from '@/api/services/schema.service'

import { useStepsStore, HoldingProperty } from '@/stores/funnels/steps'
import { useReportsStore } from '@/stores/reports/reports'
import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'
import { useBreakdownsStore } from '@/stores/reports/breakdowns'
import { useFilterGroupsStore, FilterGroup } from '@/stores/reports/filters'
import { useSegmentsStore, Segment } from '@/stores/reports/segments'
import { Each } from '@/components/uikit/UiCalendar/UiCalendar'
import { Value } from '@/api'

import {
    Property,
    EventType,
    PropertyType,
    ReportType,
    QuerySimple,
    QueryCountPerGroup,
    QueryAggregatePropertyPerGroup,
    QueryAggregateProperty,
    QueryFormula,
    EventSegmentation,
    FunnelQuery,
    QueryFormulaTypeEnum,
    QuerySimpleTypeEnum,
    QueryCountPerGroupTypeEnum,
    QueryAggregatePropertyPerGroupTypeEnum,
    QueryAggregatePropertyTypeEnum,
    EventSegmentationEvent,
    EventGroupedFiltersGroupsInner,
    EventSegmentationSegment,
    DidEventRelativeCountTypeEnum,
    SegmentConditionDidEventTypeEnum,
    SegmentConditionHadPropertyValueTypeEnum,
    SegmentConditionHasPropertyValueTypeEnum,
    TimeBetweenTypeEnum,
    Event as EventItem,
    EventFilterByProperty,
    TimeBetween,
    TimeLast,
    TimeAfterFirstUse,
    TimeWindowEach,
    TimeLastTypeEnum,
    TimeAfterFirstUseTypeEnum,
    TimeWindowEachTypeEnum,
    BreakdownByProperty,
    FunnelQueryStepsInner,
    PropertyRef,
    QueryAggregate,
    QueryAggregatePerGroup,
    EventFilterByPropertyTypeEnum,
    SegmentConditionAnd,
    SegmentConditionOr
} from '@/api'

type Queries = QuerySimple | QueryCountPerGroup | QueryAggregatePropertyPerGroup | QueryAggregateProperty | QueryFormula

import { useEventsStore, Event, EventQuery, EventBreakdown, ChartType } from '@/stores/eventSegmentation/events'
import { Step } from '@/types/steps'
import { EventRef, EventQueryRef, Condition, PropertyRef as PropertyRefEvent, UserCustomProperty } from '@/types/events'
import { Filter } from '@/types/filters'

type GetValues = {
    eventName?: string
    eventType?: EventType,
    propertyName: string
    propertyType?: PropertyType
}

type GetTime = {
    time: TimeAfterFirstUse | TimeBetween | TimeLast | TimeWindowEach
}

const getTime = (props: GetTime) => {
    let each = null
    const period = {
        from: '',
        to: '',
        last: 0,
        type: props.time.type === 'windowEach' ? 'each' : props.time.type
    }

    switch (props.time.type) {
        case TimeLastTypeEnum.Last:
            period.last = props.time.last
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
    let valuesList: Value[] = []
    const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
        propertyType: props.propertyType || PropertyType.User,
        eventType: props.eventType || EventType.Regular,
        propertyName: props.propertyName,
    });
    if (res?.data?.data) {
        valuesList = res.data.data;
    }
    return valuesList
}

const computedFilter = async (eventName: string | undefined, eventType: EventType | undefined, items: EventFilterByProperty[]) => {
    return await Promise.all(items.map(async filter => {
        return {
            propRef: {
                type: filter.propertyType as PropertyType,
                id: filter?.propertyId || 0,
                name: filter.propertyName || '',
            },
            opId: filter.operation,
            values: filter.value || [],
            valuesList: await getValues({
                eventName: eventName,
                eventType: eventType,
                propertyName: filter.propertyName || '',
                propertyType: filter.propertyType as PropertyType,
            }),
        }
    }))
}

const mapReportToEvents = async (items: EventSegmentationEvent[]): Promise<Event[]> => {
    return await Promise.all(items.map(async (item): Promise<Event> => {
        return {
            ref: {
                type: item.eventType,
                id: item.eventId || 0,
                name: item.eventName || '',
            },
            filters: item.filters ? await computedFilter(item.eventName, item.eventType, item.filters) as Filter[] : [],
            queries: item.queries.map((row, i): EventQuery => {
                const query = row as Queries

                const queryRef: EventQueryRef = {
                    type: query.type,
                }

                switch (query.type) {
                    case QueryFormulaTypeEnum.Formula:
                        queryRef.value = query.formula
                        break;
                    case QuerySimpleTypeEnum.CountEvents:
                    case QuerySimpleTypeEnum.CountUniqueGroups:
                    case QuerySimpleTypeEnum.MonthlyActiveGroups:
                    case QuerySimpleTypeEnum.WeeklyActiveGroups:
                        break;
                    case QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup:
                        queryRef.typeGroupAggregate = query.aggregatePerGroup as QueryAggregatePerGroup
                    case QueryAggregatePropertyTypeEnum.AggregateProperty:
                    case QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup:
                        queryRef.propRef = {
                            type: query.propertyType,
                            id: query.propertyId || 0,
                            name: query.propertyName || '',
                        }
                    case QueryAggregatePropertyTypeEnum.AggregateProperty:
                    case QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup:
                    case QueryCountPerGroupTypeEnum.CountPerGroup:
                        queryRef.typeAggregate = query.aggregate as QueryAggregate
                        break
                }

                return {
                    queryRef,
                    noDelete: i === 0
                }
            }),
            breakdowns: item.breakdowns ? item.breakdowns.map((row): EventBreakdown => {
                return {
                    propRef: {
                        type: row.propertyType as PropertyType,
                        id: row.propertyId || 0,
                        name: row.propertyName || '',
                    }
                }
            }) : [],
        }
    }))
}

const mapReportToFilterGroups = async (items: EventGroupedFiltersGroupsInner[]): Promise<FilterGroup[]> => {
    const commonStore = useCommonStore()

    return await Promise.all(items.map(async (item): Promise<FilterGroup> => {
        return {
            condition: item.filtersCondition,
            filters: item.filters ? await Promise.all(item.filters.map(async (filter): Promise<Filter> => {
                let valuesList: Value[] = []
                try {
                    const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
                        propertyName: filter.propertyName || '',
                        propertyType: filter.propertyType,
                        eventType: EventType.Regular,
                    })

                    if (res.data.data) {
                        valuesList = res.data.data
                    }
                } catch (error) {
                    throw new Error('error get values');
                }
                return {
                    propRef: {
                        type: filter.propertyType,
                        id: filter.propertyId || 0,
                        name: filter.propertyName || '',
                    },
                    opId: filter.operation,
                    values: filter.value || [],
                    valuesList: valuesList as string[] | boolean[] | number[],
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

                if (condition === SegmentConditionAnd.And || condition === SegmentConditionOr.Or) {
                    return {
                        action: {
                            name: i18n.t(`events.condition.${row}`),
                            id: condition,
                        },
                        filters: []
                    }
                } else {
                    const res: Condition = {
                        action: {
                            name: i18n.t(`events.condition.${condition.type}`),
                            id: condition.type || '',
                        },
                        filters: []
                    }

                    switch (condition.type) {
                        case SegmentConditionDidEventTypeEnum.DidEvent:
                            if (condition.eventName || condition.eventId) {
                                res.event = {
                                    name: condition.eventName || '',
                                    ref: {
                                        type: condition.eventType || EventType.Regular,
                                        id: condition.eventId || 0,
                                        name: condition.eventName || '',
                                    }
                                }
                            }

                            if (condition.filters) {
                                res.filters = await computedFilter(condition.eventName, condition.eventType, condition.filters)
                            }

                            if (condition.aggregate) {
                                res.opId = condition.aggregate.operation

                                if (condition.aggregate.type !== DidEventRelativeCountTypeEnum.RelativeCount && condition.aggregate?.value) {
                                    res.valueItem = condition.aggregate.value
                                }

                                if (condition.aggregate.time) {
                                    const { each, period } = getTime({ time: condition.aggregate.time });
                                    res.period = period
                                    res.each = each as Each
                                }

                                if (condition.aggregate.type === DidEventRelativeCountTypeEnum.RelativeCount && condition.aggregate.eventType) {
                                    let event: EventItem | null = null

                                    switch (condition.aggregate.eventType) {
                                        case EventType.Regular:
                                            event = lexiconStore.findEventByName(condition.aggregate.eventName || '')
                                            break
                                        case EventType.Custom:
                                            event = lexiconStore.findCustomEventById(condition.aggregate.eventId || 0)
                                            break
                                    }

                                    if (event) {
                                        res.compareEvent = {
                                            name: event.name,
                                            ref: {
                                                type: condition.aggregate.eventType as EventType,
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
                            if (condition.propertyName) {
                                const property: Property = lexiconStore.findEventPropertyByName(condition.propertyName) || lexiconStore.findUserPropertyByName(condition.propertyName)
                                res.propRef = {
                                    type: PropertyType.User,
                                    id: property?.id,
                                    name: property.name || property.displayName,
                                }
                                res.valuesList = await getValues({
                                    propertyName: property.name,
                                    propertyType: PropertyType.User,
                                })
                            }

                            res.opId = condition.operation
                            res.values = condition.value
                    }

                    return res;
                }
            }))
        }
    }))
}

const mapReportToBreakdowns = (items: BreakdownByProperty[]): EventBreakdown[] => {
    return items.map(item => {
        if (item.propertyId || item.propertyName) {
            return {
                propRef: {
                    type: item.propertyType,
                    id: item.propertyId ?? 0,
                    name: item.propertyName || '',
                }
            }
        } else {
            return {}
        }
    })
}

export const mapReportToSteps = async (items: FunnelQueryStepsInner[]): Promise<Step[]> => {
    return await Promise.all(items.map(async (item): Promise<Step> => {
        return {
            events: item.events ? await Promise.all(item.events.map(async (event) => {
                return {
                    event: {
                        type: event.eventType,
                        id: event.eventId ?? 0,
                        name: event.eventName || '',
                    },
                    filters: event.filters ? await computedFilter(event.eventName, event.eventType, event.filters) : [],
                }
            })) : []
        } as Step
    }))
}

const mapReportToHoldingConstants = (items: PropertyRef[]): HoldingProperty[] => {
    return items.map(item => {
        return {
            id: Number(item.propertyId) || 0,
            name: item?.propertyName || '',
            type: item.propertyType as EventFilterByPropertyTypeEnum,
        }
    })
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
                        type: QuerySimpleTypeEnum.CountEvents,
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

export const reportToStores = async (id: number) => {
    const reportsStore = useReportsStore()
    const eventsStore = useEventsStore()
    const filterGroupsStore = useFilterGroupsStore()
    const segmentsStore = useSegmentsStore()
    const breakdownsStore = useBreakdownsStore()
    const stepsStore = useStepsStore()

    reportsStore.reportId = id
    const report = reportsStore.activeReport
    if (report?.query) {
        if (report.type === ReportType.EventSegmentation) {
            const query = report?.query as EventSegmentation
            eventsStore.events = query.events ? await mapReportToEvents(query.events) : []
        }
        if (report.type === ReportType.Funnel) {
            const query = report?.query as FunnelQuery
            stepsStore.steps = query?.steps ? await mapReportToSteps(query.steps) : []
            stepsStore.holdingProperties = query?.holdingConstants ? mapReportToHoldingConstants(query.holdingConstants) : []
        }
        filterGroupsStore.condition = report.query?.filters?.groupsCondition || 'and'
        filterGroupsStore.filterGroups = report.query?.filters?.groups ? await mapReportToFilterGroups(report.query.filters.groups) : []
        segmentsStore.segments = report?.query?.segments ? await mapReportToSegments(report.query.segments) : []
        breakdownsStore.breakdowns = report?.query?.breakdowns ? mapReportToBreakdowns(report.query.breakdowns) : []
        eventsStore.chartType = report?.query?.chartType as ChartType
    }
}