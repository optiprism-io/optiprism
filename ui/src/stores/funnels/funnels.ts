import {defineStore} from 'pinia';
import {getLastNDaysRange} from '@/helpers/calendarHelper';
import {getYYYYMMDD} from '@/helpers/getStringDates';

type FunnelsStore = {
  controlsPeriod: string | number;
  period: {
    from: string,
    to: string,
    last: number,
    type: string,
  };
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
    }),
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
    }
})
