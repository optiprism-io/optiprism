import { defineStore } from 'pinia'
import {
    EventRef,
    PropertyRef,
    EventQueryRef,
} from '@/types/events';
import { OperationId, Value, Group } from '@/types'
import { getYYYYMMDD } from '@/helpers/getStringDates';
import { getLastNDaysRange } from '@/helpers/calendarHelper';
import {
    PropertyType,
    TimeUnit,
    EventSegmentation,
    EventRecordsListRequestTime,
    EventChartType,
    EventSegmentationEvent,
    QueryAggregatePropertyTypeEnum,
    QueryAggregatePropertyPerGroupTypeEnum,
    QueryCountPerGroupTypeEnum,
    QueryFormulaTypeEnum,
    QuerySimpleTypeEnum,
    EventFilterByProperty,
    EventType,
    QueryAggregatePerGroup,
    QueryAggregatePropertyPerGroup,
    QueryAggregateProperty,
    QueryAggregate,
    QuerySimple,
    QueryCountPerGroup,
    QueryFormula,
} from '@/api'

import { useLexiconStore } from '@/stores/lexicon'
import { useSegmentsStore } from '@/stores/reports/segments'
import { useFilterGroupsStore } from '../reports/filters'
import { useBreakdownsStore } from '../reports/breakdowns'

type Query = QuerySimple | QueryCountPerGroup | QueryAggregatePropertyPerGroup | QueryAggregateProperty | QueryFormula

export type ChartType = 'line' | 'pie' | 'column';


export interface EventFilter {
    propRef?: PropertyRef;
    opId: OperationId;
    values: Value[];
    valuesList: Value[] | []
    error?: boolean;
}

export interface EventBreakdown {
    propRef?: PropertyRef;
    error?: boolean;
}

export interface EventQuery {
    queryRef?: EventQueryRef;
    noDelete?: boolean;
}

export type Event = {
    ref: EventRef;
    filters: EventFilter[];
    breakdowns: EventBreakdown[];
    queries: EventQuery[];
};

export interface EventPayload {
    event: Event
    index: number
}

export type Events = {
    events: Event[]
    group: Group;

    controlsGroupBy: TimeUnit;
    controlsPeriod: string | number;
    period: {
        from: string,
        to: string,
        last: number,
        type: string,
    },
    compareTo: TimeUnit | string
    compareOffset: number,
    chartType: ChartType | string

    editCustomEvent: number | null
};

export const initialQuery = <EventQuery[]>[
    {
        queryRef: <EventQueryRef>{
            type: 'countEvents',
            name: 'countEvents'
        },
        noDelete: true,
    }
]

const computedEventProperties = (type: PropertyType, items: any): PropertyRef[] => {
    return items.map((item: any) => {
        return {
            type: type,
            id: item.id,
        };
    });
};

export const useEventsStore = defineStore('events', {
    state: (): Events => ({
        events: [],
        group: Group.User,

        controlsGroupBy: 'day',
        controlsPeriod: '30',
        period: {
            from: '',
            to: '',
            type: 'last',
            last: 30,
        },
        compareTo: '',
        compareOffset: 1,
        chartType: 'line',

        editCustomEvent: null,
    }),
    getters: {
        timeRequest(): EventRecordsListRequestTime {
            switch (this.period.type) {
                case 'last':
                    return {
                        type: this.period.type,
                        last: this.period.last,
                        unit: 'day'
                    }
                case 'since':
                    return {
                        type: 'from',
                        from: this.period.from,
                    }
                case 'between':
                    return {
                        type: this.period.type,
                        from: this.period.from,
                        to: this.period.to,
                    }
                default:
                    return {
                        type: 'last',
                        last: Number(this.controlsPeriod),
                        unit: 'day'
                    }
            }
        },
        hasSelectedEvents(): boolean {
            return Array.isArray(this.events) && Boolean(this.events.length)
        },
        allSelectedEventPropertyRefs() {
            const lexiconStore = useLexiconStore();
            const items: PropertyRef[] = []

            this.events.forEach(item => {
                const eventRef = item.ref;
                items.push(...computedEventProperties(PropertyType.Event, lexiconStore.findEventProperties(eventRef)));
                items.push(...computedEventProperties(PropertyType.Custom, lexiconStore.findEventCustomProperties(eventRef)));
            });

            return [
                ...new Set(items),
                ...computedEventProperties(PropertyType.User, lexiconStore.userProperties),
            ];
        },
        propsForEventSegmentationResult(): EventSegmentation {
            const lexiconStore = useLexiconStore()
            const filterGroupsStore = useFilterGroupsStore()
            const breakdownsStore = useBreakdownsStore()
            const segmentsStore = useSegmentsStore()

            const props: EventSegmentation = {
                time: this.timeRequest,
                group: this.group,
                intervalUnit: this.controlsGroupBy,
                chartType: this.chartType as EventChartType,
                analysis: {
                    type: 'linear',
                },
                events: this.events.map((item): EventSegmentationEvent => {
                    const eventLexicon = lexiconStore.findEvent(item.ref)

                    return {
                        eventName: eventLexicon.name,
                        queries: item.queries.filter(query => query.queryRef).map((query): Query => {
                            const type = query.queryRef?.type;

                            if (query?.queryRef?.propRef) {
                                const prop = {
                                    type: type,
                                    propertyType: query.queryRef.propRef.type,
                                    propertyId: query.queryRef.propRef.id,
                                    aggregate: query.queryRef.typeAggregate || QueryAggregate.Avg,
                                }

                                if (type === QueryAggregatePropertyTypeEnum.AggregateProperty) {
                                    return {
                                        ...prop as QueryAggregateProperty,
                                        type: QueryAggregatePropertyTypeEnum.AggregateProperty,
                                    }
                                }

                                if (type === QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup && query.queryRef.typeGroupAggregate) {
                                    return {
                                        ...prop,
                                        aggregatePerGroup: query.queryRef.typeGroupAggregate || QueryAggregate.Avg,
                                        type: QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup,
                                    }
                                }
                            }

                            if (query?.queryRef?.type === QueryCountPerGroupTypeEnum.CountPerGroup) {
                                return {
                                    type: QueryCountPerGroupTypeEnum.CountPerGroup,
                                    aggregate: query.queryRef.typeAggregate || QueryAggregate.Avg,
                                }
                            }

                            if (query.queryRef?.type === QueryFormulaTypeEnum.Formula) {
                                return {
                                    type: QueryFormulaTypeEnum.Formula,
                                    formula: query.queryRef.value || '',
                                }
                            }

                            return {
                                type: query.queryRef?.type,
                            } as QuerySimple
                        }),
                        eventType: item.ref.type as EventType,
                        eventId: item.ref.id,
                        filters: item.filters.filter(item => item.propRef).map((filter): EventFilterByProperty => {
                            const ref = filter.propRef;
                            const propertyId = ref?.id || 0;
                            const eventProperty = ref?.name ? lexiconStore.findEventPropertyByName(ref.name) : lexiconStore.findEventPropertyById(propertyId);
                            let name = '';

                            switch (filter.propRef?.type || 'event') {
                                case PropertyType.Event:
                                    name = ref?.name || eventProperty.name;
                                    break;
                                case PropertyType.Custom:
                                    name = lexiconStore.findEventCustomPropertyById(propertyId)?.name || ''
                                    break;
                                case PropertyType.User:
                                    name = lexiconStore.findUserPropertyById(propertyId).name
                                    break;
                            }

                            return {
                                type: 'property',
                                propertyName: name,
                                propertyId,
                                propertyType: filter.propRef?.type || 'event',
                                operation: filter.opId,
                                value: filter.values,
                            }
                        }),
                    }
                }),
                filters: filterGroupsStore.filters,
                segments: segmentsStore.segmentationItems,
                breakdowns: breakdownsStore.breakdownsItems
            }

            return props
        },
    },
    actions: {
        setEditCustomEvent(payload: number | null) {
            this.editCustomEvent = payload
        },
        setEvent(payload: EventPayload) {
            this.events[payload.index] = payload.event
        },
        initPeriod(): void {
            const lastNDateRange = getLastNDaysRange(20);
            this.period = {
                from: getYYYYMMDD(lastNDateRange.from),
                to: getYYYYMMDD(new Date()),
                type: 'last',
                last: 20,
            };
        },
        addEventByRef(ref: EventRef, initQuery?: boolean): void {
            switch (ref.type) {
                case EventType.Regular:
                    this.addEvent(ref.id, initQuery);
                    break;
                case EventType.Custom:
                    this.addCustomEvent(ref.id);
                    break;
            }
        },
        addEvent(payload: number, initQuery = true): void {
            this.events.push(<Event>{
                ref: <EventRef>{
                    type: EventType.Regular,
                    id: payload
                },
                filters: [],
                breakdowns: [],
                queries: initQuery ? initialQuery : [],
            });
        },
        addCustomEvent(payload: number): void {
            this.events.push(<Event>{
                ref: <EventRef>{
                    type: EventType.Custom,
                    id: payload
                },
                filters: [],
                breakdowns: [],
                queries: initialQuery,
            });
        },
        deleteEvent(idx: number): void {
            this.events.splice(idx, 1);
        },

        /**
         * Breakdown
         *
         * You cannot create two identical groupings.
         * The grouping can be removed by hovering and clicking on the cross.
         *
         * @func removeBreakdown
         * @func addBreakdown
         * @func changeBreakdownProperty
         */
        removeBreakdown(eventIdx: number, breakdownIdx: number): void {
            this.events[eventIdx].breakdowns.splice(breakdownIdx, 1);
        },
        addBreakdown(idx: number): void {
            const emptyBreakdown = this.events[idx].breakdowns.findIndex((breakdown): boolean => breakdown.propRef === undefined);

            if (emptyBreakdown !== -1) {
                this.removeBreakdown(idx, emptyBreakdown);
            }

            this.events[idx].breakdowns.push(<EventFilter>{
                propRef: undefined,
            });
        },
        changeBreakdownProperty(eventIdx: number, breakdownIdx: number, propRef: PropertyRef) {
            this.events[eventIdx].breakdowns[breakdownIdx] = <EventFilter>{
                propRef: propRef,
            };
        },

        /**
         * Query
         *
         * @func removeQuery
         * @func addQuery
         * @func changeQuery
         */
        removeQuery(eventIdx: number, queryIdx: number): void {
            this.events[eventIdx].queries.splice(queryIdx, 1);
        },
        addQuery(idx: number): void {
            const emptyQueryIndex = this.events[idx].queries.findIndex((query): boolean => query.queryRef === undefined);

            if (emptyQueryIndex !== -1) {
                this.removeQuery(idx, emptyQueryIndex);
            }

            this.events[idx].queries.push(<EventQuery>{
                queryRef: undefined,
            });
        },
        changeQuery(eventIdx: number, queryIdx: number, queryRef: EventQueryRef) {
            const queries = [...this.events[eventIdx].queries];

            queries[queryIdx] = <EventQuery>{
                queryRef: queryRef,
                noDelete: queryIdx === 0,
            };

            this.events[eventIdx].queries = queries;
        },
    },
});
