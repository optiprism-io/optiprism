import { defineStore } from "pinia";
import {
    EventRef,
    PropertyRef,
    EventQueryRef,
    EVENT_TYPE_REGULAR,
    EVENT_TYPE_CUSTOM
} from "@/types/events";
import { OperationId, Value, Group } from '@/types'
import queriesService, { EventSegmentation } from "@/api/services/queries.service";
import { getYYYYMMDD, getStringDateByFormat } from "@/helpers/getStringDates";
import { getLastNDaysRange } from "@/helpers/calendarHelper";
import {Column, Row} from "@/components/uikit/UiTable/UiTable";
import { PropertyType, TimeUnit } from '@/api'

import { useLexiconStore } from "@/stores/lexicon";
import { useSegmentsStore } from "@/stores/eventSegmentation/segments";

const COLUMN_WIDTH = 170;
export type ChartType = 'line' | 'pie' | 'column';

type ColumnMap = {
    [key: string]: Column;
}

export interface EventFilter {
    propRef?: PropertyRef;
    opId: OperationId;
    values: Value[];
    valuesList: string[] | []
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

    controlsGroupBy: string;
    controlsPeriod: string | number;
    period: {
        from: string,
        to: string,
        last: number,
        type: string,
    },
    compareTo: TimeUnit | string
    compareOffset: number,
    chartType: ChartType | string,

    eventSegmentation: any // TODO integrations backend
    eventSegmentationLoading: boolean

    editCustomEvent: number | null
};

export const initialQuery = <EventQuery[]>[
    {
        queryRef: <EventQueryRef>{
            type: "simple",
            name: "countEvents"
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

export const useEventsStore = defineStore("events", {
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

        eventSegmentation: {
            series: [],
        },
        eventSegmentationLoading: false,

        editCustomEvent: null,
    }),
    getters: {
        hasSelectedEvents(): boolean {
            return Array.isArray(this.events) && Boolean(this.events.length)
        },
        hasDataEvent(): boolean {
            return this.eventSegmentation && Array.isArray(this.eventSegmentation.series) && this.eventSegmentation.series.length;
        },
        tableColumns(): ColumnMap {
            if (this.hasDataEvent) {
                return {
                    ...this.eventSegmentation.dimensionHeaders.reduce((acc: any, key: string, i: number) => {
                        acc[key] = {
                            pinned: true,
                            value: key,
                            title: key,
                            truncate: true,
                            lastPinned: this.eventSegmentation.dimensionHeaders.length - 1 === i,
                            left: i * COLUMN_WIDTH,
                        }

                        return acc
                    }, {}),
                    ...this.eventSegmentation.metricHeaders.reduce((acc: any, key: string) => {
                        acc[key] = {
                            value: key,
                            title: getStringDateByFormat(key, '%d %b, %Y'),
                        }

                        return acc
                    }, {})
                }
            } else {
                return {};
            }
        },
        tableData(): Row[] {
            if (this.hasDataEvent) {
                return this.eventSegmentation.series.map((values: number[], indexSeries: number) => {
                    const items = this.eventSegmentation.dimensions[indexSeries];

                    return [
                        ...items.map((dimension: string, i: number) => {
                            return {
                                value: dimension,
                                title: dimension,
                                pinned: true,
                                lastPinned: i === items.length - 1,
                                left: i * COLUMN_WIDTH,
                            };
                        }),
                        ...values.map((value: number | undefined) => {
                            return {
                                value,
                                title: value || '-',
                            };
                        })
                    ];
                });
            } else {
                return [];
            }
        },
        tableColumnsValues(): Column[] {
            return Object.values(this.tableColumns);
        },
        lineChart(): any[] {
            if (this.hasDataEvent) {
                return this.eventSegmentation.series.reduce((acc: any[], item: number[], indexSeries: number) => {
                    item.forEach((value: number, indexValue: number) => {
                        acc.push({
                            date: new Date(this.eventSegmentation.metricHeaders[indexValue]),
                            value,
                            category: this.eventSegmentation.dimensions[indexSeries].filter((item: string) => item !== '-').join(', '),
                        });
                    });
                    return acc;
                }, []);
            } else {
                return [];
            }
        },
        pieChart(): any[] {
            if (this.hasDataEvent) {
                return this.eventSegmentation.singles.map((item: number, index: number) => {
                    return {
                        type: this.eventSegmentation.dimensions[index].filter((item: string) => item !== '-').join(', '),
                        value: item,
                    };
                });
            } else {
                return [];
            }
        },
        noDataLineChart() {
            return !this.lineChart.length
        },
        allSelectedEventPropertyRefs() {
            const lexiconStore = useLexiconStore();
            const items: PropertyRef[] = []

            this.events.forEach(item => {
                const eventRef = item.ref;
                items.push(...computedEventProperties(PropertyType.Event, lexiconStore.findEventProperties(eventRef.id)));
                items.push(...computedEventProperties(PropertyType.Custom, lexiconStore.findEventCustomProperties(eventRef.id)));
            });

            return [
                ...new Set(items),
                ...computedEventProperties(PropertyType.User, lexiconStore.userProperties),
            ];
        },
        propsForEventSegmentationResult(): EventSegmentation {
            const lexiconStore = useLexiconStore();
            const segmentsStore = useSegmentsStore()

            let time = {
                from: new Date(this.period.from),
                to: new Date(this.period.to),
                type: this.period.type,
            };

            if (this.controlsPeriod !== 'calendar') {
                switch(this.controlsGroupBy) {
                    case 'day':
                        time = {
                            ...getLastNDaysRange(Number(this.controlsPeriod) + 1),
                            type: 'last',
                        };
                        break;
                    case 'month':
                        time = {
                            ...getLastNDaysRange(Number(this.controlsPeriod) * 30),
                            type: 'last',
                        };
                        break;
                    case 'week':
                        time = {
                            ...getLastNDaysRange(Number(this.controlsPeriod) * 7),
                            type: 'last',
                        };
                        break;
                    case 'hour':
                        // TODO
                        break;
                    case 'minuts':
                        // TODO
                        break;
                }
            }

            const props: EventSegmentation = {
                time,
                group: this.group,
                intervalUnit: this.controlsGroupBy,
                chartType: this.chartType,
                events: this.events.map(item => {
                    const eventLexicon = lexiconStore.findEvent(item.ref)

                    const event = {
                        eventName: eventLexicon.name,
                        eventType: item.ref.type,
                        queries: item.queries.map(query => {
                            const queryLexicon = lexiconStore.findQuery(query.queryRef)

                            return {
                                queryType: queryLexicon ? queryLexicon.type : '',
                            }
                        }),
                    }

                    return event;
                }),
                segments: segmentsStore.segments.length ? segmentsStore.segments : null,
            };

            if (this.compareTo) {
                props.compare = {
                    offset: 1,
                    unit: this.compareTo,
                }
            }

            return props;
        },
        isNoData(): boolean {
            return !this.eventSegmentationLoading &&
                (!this.eventSegmentation || (this.eventSegmentation && Array.isArray(this.eventSegmentation.series) && !this.eventSegmentation.series.length))
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
        async fetchEventSegmentationResult() {
            this.eventSegmentationLoading = true;
            try {
                const res = await queriesService.eventSegmentation(this.propsForEventSegmentationResult);
                if (res) {
                    this.eventSegmentation = res;
                }
            } catch (error) {
                throw new Error("error getEventsValues");
            }
            this.eventSegmentationLoading = false;
        },

        addEventByRef(ref: EventRef): void {
            switch (ref.type) {
                case EVENT_TYPE_REGULAR:
                    this.addEvent(ref.id);
                    break;
                case EVENT_TYPE_CUSTOM:
                    this.addCustomEvent(ref.id);
                    break;
            }
        },
        addEvent(id: number | string): void {
            this.events.push(<Event>{
                ref: <EventRef>{ type: EVENT_TYPE_REGULAR, id: id },
                filters: [],
                breakdowns: [],
                queries: initialQuery,
            });
        },
        addCustomEvent(id: number | string): void {
            this.events.push(<Event>{
                ref: <EventRef>{ type: EVENT_TYPE_CUSTOM, id: id },
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
