import {defineStore} from 'pinia';
import {getLastNDaysRange} from '@/helpers/calendarHelper';
import {getYYYYMMDD} from '@/helpers/getStringDates';
import { DataTableResponseColumns } from '@/api';
import dataService from '@/api/services/datas.service';

const convertColumns = (columns: DataTableResponseColumns[], stepNumbers: number[]): number[][] => {
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
  reports: DataTableResponseColumns[];
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
            this.loading = true

            try {
                const res = await dataService.funnelQuery()

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
