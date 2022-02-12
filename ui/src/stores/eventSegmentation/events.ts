import { defineStore } from "pinia";
import {
    EventRef,
    EventType,
    PropertyRef,
    PropertyType,
    EventQueryRef,
} from "@/types/events";
import { OperationId, Value, Group } from "@/types";
import schemaService from "@/api/services/schema.service";
import queriesService, { EventSegmentation } from "@/api/services/queries.service";
import { useLexiconStore } from "@/stores/lexicon";
import { getYYYYMMDD, getStringDateByFormat } from "@/helpers/getStringDates";
import { getLastNDaysRange } from "@/helpers/calendarHelper";
import { TimeUnit } from "@/types";
import {Column, Row} from "@/components/uikit/UiTable/UiTable";

export type ChartType = 'line' | 'pie' | 'column';

type ColumnMap = {
    [key: string]: Column;
}

export interface EventFilter {
    propRef?: PropertyRef;
    opId: OperationId;
    values: Value[];
    valuesList: string[];
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

export type Events = {
    events: Event[];
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

    eventSegmentation: any // TODO
    eventSegmentationLoading: boolean
};

const initialQuery = <EventQuery[]>[
    {
        queryRef: <EventQueryRef>{
            type: "simple",
            name: "countEvents"
        },
        noDelete: true,
    }
]

const compudeEventProperties = (type: PropertyType, items: any): PropertyRef[] => {
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
    }),
    getters: {
        hasDataEvent(): boolean {
            return this.eventSegmentation && Array.isArray(this.eventSegmentation.series) && this.eventSegmentation.series.length;
        },
        tableColumns(): ColumnMap {
            if (this.hasDataEvent) {
                return {
                    ...this.eventSegmentation.dimensionHeaders.reduce((acc: any, key: string) => {
                        acc[key] = {
                            value: key,
                            title: key,
                        }

                        if (key === 'Event Name') {
                            acc[key].pinned = true;
                            acc[key].lastPinned = true;
                            acc[key].truncate = true;
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
                    return [
                        ...this.eventSegmentation.dimensions[indexSeries].map((dimension: string, i: number) => {
                            return {
                                value: dimension,
                                title: dimension,
                                pinned: i === 0,
                                lastPinned: i === 0,
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
                            category: this.eventSegmentation.dimensions[indexSeries].join(', '),
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
                        type: this.eventSegmentation.dimensions[index].join(', '),
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
                items.push(...compudeEventProperties(PropertyType.Event, lexiconStore.findEventProperties(eventRef.id)));
                items.push(...compudeEventProperties(PropertyType.EventCustom, lexiconStore.findEventCustomProperties(eventRef.id)));
            });

            return [
                ...new Set(items),
                ...compudeEventProperties(PropertyType.User, lexiconStore.userProperties),
                ...compudeEventProperties(PropertyType.UserCustom, lexiconStore.userCustomProperties),
            ];
        },
        propsForEventSegmentationResult(): EventSegmentation {
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
            };

            if (this.compareTo) {
                props.compare = {
                    offset: 1,
                    unit: this.compareTo,
                }
            }

            return props;
        },
    },
    actions: {
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

        changeEvent(index: number, ref: EventRef): void {
            this.events[index] = <Event>{
                ref: ref,
                filters: [],
                breakdowns: [],
                queries: initialQuery,
            };
        },
        addEventByRef(ref: EventRef): void {
            switch (ref.type) {
                case EventType.Regular:
                    this.addEvent(ref.id);
                    break;
                case EventType.Custom:
                    this.addCustomEvent(ref.id);
                    break;
            }
        },
        addEvent(id: number): void {
            this.events.push(<Event>{
                ref: <EventRef>{ type: EventType.Regular, id: id },
                filters: [],
                breakdowns: [],
                queries: initialQuery,
            });
        },
        addCustomEvent(id: number): void {
            this.events.push(<Event>{
                ref: <EventRef>{ type: EventType.Custom, id: id },
                filters: [],
                breakdowns: [],
                queries: initialQuery,
            });
        },
        deleteEvent(idx: number): void {
            this.events.splice(idx, 1);
        },

        /**
         * Filters
         */
        addFilter(idx: number): void {
            const emptyFilter = this.events[idx].filters.find((filter): boolean => filter.propRef === undefined);

            if (emptyFilter) {
                return;
            }
            this.events[idx].filters.push(<EventFilter>{
                opId: OperationId.Eq,
                values: [],
                valuesList: []
            });
        },
        removeFilter(eventIdx: number, filterIdx: number): void {
            this.events[eventIdx].filters.splice(filterIdx, 1);
        },
        async changeFilterProperty(eventIdx: number, filterIdx: number, propRef: PropertyRef) {
            const lexiconStore = useLexiconStore();
            const eventRef: EventRef = this.events[eventIdx].ref;
            let valuesList: string[] = [];

            try {
                const res = await schemaService.propertiesValues({
                    event_name: lexiconStore.eventName(eventRef),
                    event_type: EventType[eventRef.type],
                    property_name: lexiconStore.propertyName(propRef),
                    property_type: PropertyType[propRef.type]
                });
                if (res) {
                    valuesList = res;
                }
            } catch (error) {
                throw new Error("error getEventsValues");
            }

            this.events[eventIdx].filters[filterIdx] = <EventFilter>{
                propRef: propRef,
                opId: OperationId.Eq,
                values: [],
                valuesList: valuesList
            };
        },
        changeFilterOperation(eventIdx: number, filterIdx: number, opId: OperationId): void {
            this.events[eventIdx].filters[filterIdx].opId = opId;
            this.events[eventIdx].filters[filterIdx].values = [];
        },
        addFilterValue(eventIdx: number, filterIdx: number, value: Value): void {
            this.events[eventIdx].filters[filterIdx].values.push(value);
        },
        removeFilterValue(eventIdx: number, filterIdx: number, value: Value): void {
            this.events[eventIdx].filters[filterIdx].values =
                this.events[eventIdx].filters[filterIdx].values.filter(v =>  v !== value);
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
