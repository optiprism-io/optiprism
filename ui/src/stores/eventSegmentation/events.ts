import { defineStore } from 'pinia';
import {
    EventRef,
    PropertyRef,
    EventQueryRef,
} from '@/types/events';
import { OperationId, Value, Group } from '@/types'
import queriesService, { EventSegmentation } from '@/api/services/queries.service';
import { getYYYYMMDD, getStringDateByFormat } from '@/helpers/getStringDates';
import { getLastNDaysRange } from '@/helpers/calendarHelper';
import {Column, Row} from '@/components/uikit/UiTable/UiTable';
import { PropertyType, TimeUnit, EventType, DataTableResponse, DataTableResponseColumns } from '@/api'

import { useLexiconStore } from '@/stores/lexicon';
import { useSegmentsStore } from '@/stores/eventSegmentation/segments';

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
    chartType: ChartType | string

    eventSegmentation: DataTableResponse
    eventSegmentationLoading: boolean

    editCustomEvent: number | null
};

export const initialQuery = <EventQuery[]>[
    {
        queryRef: <EventQueryRef>{
            type: 'simple',
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

        eventSegmentation: {
            columns: [],
        },
        eventSegmentationLoading: false,

        editCustomEvent: null,
    }),
    getters: {
        hasSelectedEvents(): boolean {
            return Array.isArray(this.events) && Boolean(this.events.length)
        },
        hasDataEvent(): boolean {
            return this.eventSegmentation && Array.isArray(this.eventSegmentation.columns) && Boolean(this.eventSegmentation.columns.length);
        },
        dimensionColumns(): DataTableResponseColumns[] {
            return this.eventSegmentation?.columns ? this.eventSegmentation.columns.filter(column => column.type === 'dimension') : [];
        },
        metricColumns(): DataTableResponseColumns[] {
            return this.eventSegmentation?.columns ? this.eventSegmentation.columns.filter(column => column.type === 'metric') : [];
        },
        metricValueColumns(): DataTableResponseColumns[] {
            return this.eventSegmentation?.columns ? this.eventSegmentation.columns.filter(column => column.type === 'metricValue') : [];
        },
        totalColumn(): DataTableResponseColumns | undefined {
            return this.metricValueColumns.find(item => item.name === 'total')
        },
        sortedColumns(): DataTableResponseColumns[] {
            return [
                ...this.dimensionColumns,
                ...this.metricValueColumns,
                ...this.metricColumns,
            ]
        },
        tableColumns(): ColumnMap {
            if (this.eventSegmentation?.columns) {
                return {
                    ...this.dimensionColumns.reduce((acc: any, column: DataTableResponseColumns, i: number) => {
                        if (column.name) {
                            acc[column.name] = {
                                pinned: true,
                                value: column.name,
                                title: column.name,
                                truncate: true,
                                lastPinned: this.dimensionColumns.length - 1 === i,
                                style: {
                                    left: i ? `${i * COLUMN_WIDTH}px` : '',
                                    width: 'auto',
                                    minWidth: i === 0 ? `${COLUMN_WIDTH}x` : '',
                                },
                            }
                        }

                        return acc
                    }, {}),
                    ...this.metricValueColumns.reduce((acc: any, column: DataTableResponseColumns) => {
                        if (column.name) {
                            acc[column.name] = {
                                value: column.name,
                                title: getStringDateByFormat(column.name, '%d %b, %Y'),
                            }
                        }
                        return acc
                    }, {}),
                    ...this.metricColumns.reduce((acc: any, column: DataTableResponseColumns) => {
                        if (column.name) {
                            acc[column.name] = {
                                value: column.name,
                                title: column.name,
                            }
                        }
                        return acc
                    }, {})
                }
            } else {
                return {};
            }
        },
        tableData(): Row[] {
            if (this.eventSegmentation?.columns) {
                return this.sortedColumns.reduce((tableRows: Row[], column: DataTableResponseColumns, indexColumn: number) => {
                    const left = indexColumn ? `${indexColumn * COLUMN_WIDTH}px` : '';
                    const minWidth = indexColumn === 0 ? `${COLUMN_WIDTH}px` : '';

                    if (column.values) {
                        column.values.forEach((item, i) => {

                            if (!tableRows[i]) {
                                tableRows[i] = [];
                            }

                            if (column.type === 'dimension') {
                                tableRows[i].push({
                                    value: item || '',
                                    title: item || '-',
                                    pinned: true,
                                    lastPinned: indexColumn === this.dimensionColumns.length - 1,
                                    style: {
                                        left,
                                        width: 'auto',
                                        minWidth,
                                    },
                                })
                            } else {
                                tableRows[i].push({
                                    value: item,
                                    title: item || '-',
                                });
                            }
                        })
                    }

                    return tableRows
                }, []);
            } else {
                return [];
            }
        },
        tableColumnsValues(): Column[] {
            return Object.values(this.tableColumns);
        },
        lineChart(): any[] {
            if (this.hasDataEvent) {
                return this.metricValueColumns.reduce((acc: any[], item: DataTableResponseColumns) => {
                    if (item.values && item.name !== 'total') {
                        item.values.forEach((value, indexValue: number) => {

                            acc.push({
                                date: item.name ? new Date(item.name) : '',
                                value,
                                category: this.dimensionColumns.map((column: DataTableResponseColumns) => {
                                    return column.values ? column.values[indexValue] : ''
                                }).filter(item => Boolean(item)).join(', '),
                            });
                        });
                    }
                    return acc;
                }, []);
            } else {
                return [];
            }
        },
        pieChart(): any[] {
            if (this.hasDataEvent && this.totalColumn?.values) {
                return this.totalColumn.values.map((item, index: number) => {
                    return {
                        type: this.dimensionColumns.map((column: DataTableResponseColumns) => {
                            return column.values ? column.values[index] : ''
                        }).filter(item => Boolean(item)).join(', '),
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
                (!this.eventSegmentation || (this.eventSegmentation && Array.isArray(this.eventSegmentation.columns) && !this.eventSegmentation.columns.length))
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
                const res: DataTableResponse = await queriesService.eventSegmentation(this.propsForEventSegmentationResult);
                if (res) {
                    this.eventSegmentation = res;
                }
            } catch (error) {
                throw new Error('error getEventsValues');
            }
            this.eventSegmentationLoading = false;
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
        addEvent(payload: number): void {
            this.events.push(<Event>{
                ref: <EventRef>{
                    type: EventType.Regular,
                    id: payload
                },
                filters: [],
                breakdowns: [],
                queries: initialQuery,
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
