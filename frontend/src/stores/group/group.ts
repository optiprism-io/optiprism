import { defineStore } from 'pinia';
import { GroupRecord, EventRecordsListRequestTime } from '@/api';
import { groupRecordsService } from '@/api/services/groupRecords.service';
import { useCommonStore } from '@/stores/common';

export type Group = {
    users: GroupRecord[],
    loadingList: boolean,
    controlsPeriod: string | number;
    period: {
        from: string,
        to: string,
        last: number,
        type: string,
    },
};

export const useGroupStore = defineStore('groupStore', {
    state: (): Group => ({
        users: [],
        loadingList: false,
        controlsPeriod: '30',
        period: {
            from: '',
            to: '',
            type: 'last',
            last: 30,
        },
    }),
    actions: {
        async getList() {
            const commonStore = useCommonStore();
            try {
                const res = await groupRecordsService.getList(commonStore.organizationId, commonStore.organizationId, {
                    time: this.timeRequest,
                    group: 'users',
                    // TODO  segments: EventSegmentationSegment[] / filters: EventGroupedFilters
                });

                if (res?.data?.data) {
                    this.users = res.data.data
                }
            } catch (e) {
                console.error('error update event property');
            }
        },
        get() {
            // TODO
        },
        update() {
            // TODO
        },
    },
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
    },
});