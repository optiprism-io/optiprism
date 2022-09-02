import {defineStore} from 'pinia';
import {getLastNDaysRange} from '@/helpers/calendarHelper';
import {getYYYYMMDD} from '@/helpers/getStringDates';
import {
    BreakdownByProperty,
    DataTableResponseColumnsInner,
    EventFilterByProperty,
    FunnelEvent,
    FunnelEventEventTypeEnum,
    TimeBetween,
    TimeFrom,
    TimeLast
} from '@/api';
import dataService from '@/api/services/datas.service';
import {useCommonStore} from '@/stores/common';
import {useStepsStore} from '@/stores/funnels/steps';
import {useEventName} from '@/helpers/useEventName';
import { useFilterGroupsStore } from '@/stores/reports/filters'
import { useBreakdownsStore } from '@/stores/reports/breakdowns'
import { useSegmentsStore } from '@/stores/reports/segments'

const convertColumns = (columns: DataTableResponseColumnsInner[], stepNumbers: number[]): number[][] => {
    const result: number[][] = []

    for (let i = 0; i < stepNumbers.length; i++) {
        const column = columns.find(col => col.step === stepNumbers[i])
        if (column) {
            result.push(column.values as number[])
        } else {
            result.push([])
        }
    }

    return result
}

type FunnelsStore = {
  controlsPeriod: string | number;
  period: {
    from: string,
    to: string,
    last: number,
    type: string,
  };
  reports: DataTableResponseColumnsInner[];
  loading: boolean;
}

export const useFunnelsStore = defineStore('funnels', {
    state: (): FunnelsStore => ({
        controlsPeriod: '30',
        period: {
            from: '',
            to: '',
            type: 'last',
            last: 30,
        },
        reports: [],
        loading: false,
    }),
    getters: {
        timeRequest(): TimeBetween | TimeFrom | TimeLast {
            switch (this.period.type) {
                case 'last':
                    return {
                        type: this.period.type,
                        n: this.period.last,
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
                        n: Number(this.controlsPeriod),
                        unit: 'day'
                    }
            }
        },
        stepNumbers(): number[] {
            const metricValueColumns = this.reports.filter(col => col.type === 'funnelMetricValue')
            const stepNumbers = metricValueColumns.map(col => col.step) as number[]
            return [...new Set(stepNumbers)]
        },
        dimensions(): string[] {
            const result: string[] = []
            const columns = this.reports.filter(col => col.type === 'dimension')

            for (let i = 0; i < (columns[0]?.values?.length ?? 0); i++) {
                const row: string[] = []
                columns.forEach(item => {
                    row.push(`${item.values?.[i] ?? ''}`)
                })
                result.push(row.join(' / '))
            }

            return result
        },
        conversionCount(): number[][] {
            const columns = this.reports.filter(col => col.name === 'conversionCount')
            return convertColumns(columns, this.stepNumbers)
        },
        conversionRatio(): number[][] {
            const columns = this.reports.filter(col => col.name === 'conversionRatio')
            return convertColumns(columns, this.stepNumbers)
        },
        dropOffCount(): number[][] {
            const columns = this.reports.filter(col => col.name === 'dropOffCount')
            return convertColumns(columns, this.stepNumbers)
        },
        dropOffRatio(): number[][] {
            const columns = this.reports.filter(col => col.name === 'dropOffRatio')
            return convertColumns(columns, this.stepNumbers)
        },
    },
    actions: {
        setControlsPeriod(payload: string) {
            this.controlsPeriod = payload;
        },
        setPeriod(payload: {from: string, to: string, type: string, last: number}) {
            this.period = payload;
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
        async getReports(): Promise<void> {
            const commonStore = useCommonStore()
            const stepsStore = useStepsStore()
            const eventName = useEventName()
            const breakdownsStore = useBreakdownsStore()
            const filterGroupsStore = useFilterGroupsStore()
            const segmentsStore = useSegmentsStore()

            this.loading = true

            try {
                const res = await dataService.funnelQuery(commonStore.organizationId, commonStore.projectId, {
                    time: this.timeRequest,
                    timeWindow: {
                        n: stepsStore.size,
                        unit: stepsStore.unit,
                    },
                    steps: stepsStore.steps.map(item => {
                        const events = item.events.map(event => {
                            return {
                                eventType: event.event.type,
                                eventId: event.event.id,
                                eventName: eventName(event.event),
                                filters: event.filters.map(filter => {
                                    return {
                                        propertyType: filter.propRef?.type ?? '',
                                        type: 'property',
                                        operation: filter.opId,
                                        value: filter.values
                                    }
                                })
                            }
                        }) as FunnelEvent[]

                        return {
                            events,
                            order: 'any',
                        }
                    }),
                    holdingConstants: stepsStore.holdingProperties.map(item => {
                        return {
                            propertyType: 'custom',
                            propertyId: item.id,
                            propertyName: item.name
                        }
                    }),
                    exclude: stepsStore.excludedEvents.map(item => {
                        return {
                            eventType: item.event.type as FunnelEventEventTypeEnum,
                            eventName: eventName(item.event),
                            filters: item.filters.map(filter => {
                                return {
                                    propertyType: filter.propRef?.type ?? '',
                                    type: 'property',
                                    operation: filter.opId,
                                    value: filter.values
                                }
                            }) as EventFilterByProperty[],
                        }
                    }),
                    filters: filterGroupsStore.filters,
                    breakdowns: breakdownsStore.breakdownsItems,
                    segments: segmentsStore.segmentationItems,
                })

                if (res?.data?.columns) {
                    this.reports = res.data.columns
                }
            } catch (e) {
                throw new Error('Error while getting funnel reports')
            } finally {
                this.loading = false
            }
        },
    }
})
