import { defineStore } from 'pinia'
import { EventType, EventFilterByProperty, TimeBetween, TimeFrom, TimeLast, EventRef } from '@/api'
import { Event } from '@/stores/eventSegmentation/events'
import dataService from '@/api/services/datas.service'
import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'

export interface Report {
    name: string
    properties: {
        [key: string]: string | number
    }
    userProperties: {
        [key: string]: string | number
    }
    matchedCustomEvents: Array<{
        id: number | string
    }>
}

type LiveStream = {
    events: Event[],
    controlsPeriod: string
    period: {
        from: string
        to: string
        last: number
        type: string
    },
    reports: Array<Report | object> | any
    activeColumns: string[]
    defaultColumns: string[]
    loading: boolean
}

export const useLiveStreamStore = defineStore('liveStream', {
    state: (): LiveStream => ({
        events: [],
        controlsPeriod: '90',
        period: {
            from: '',
            to: '',
            type: 'last',
            last: 30,
        },
        reports: [],
        activeColumns: [],
        defaultColumns: ['eventName', 'createdAt'],
        loading: false,
    }),
    getters: {
        isPeriodActive(): boolean {
            return Boolean(this.period.from) && Boolean(this.period.to) && this.controlsPeriod === 'calendar'
        },
        filtersRequest(): Array<EventFilterByProperty> {
            const filters: Array<EventFilterByProperty> = []

            this.events.forEach(event => {
                if (event.filters.length) {
                    event.filters.forEach(filter => {
                        if (filter.propRef) {
                            filters.push({
                                filterType: 'property',
                                propertyType: filter.propRef.type,
                                propertyId: filter.propRef.id,
                                operation: filter.opId,
                                value: filter.values,
                            })
                        }
                    })
                }
            })

            return filters
        },
        eventsRequest(): Array<EventRef & object> {
            const lexiconStore = useLexiconStore()

            return this.events.map(event => {
                const eventStore = lexiconStore.findEvent(event.ref)

                switch (event.ref.type) {
                    case EventType.Regular:
                        return {
                            eventName: eventStore.name,
                            eventType: event.ref.type
                        }
                    case EventType.Custom:
                        return {
                            eventId: eventStore.id,
                            eventType: event.ref.type
                        }
                }
            })
        },
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
        columnsMap(): string[] {
            const items: string[] = []

            this.reports.forEach((item: Report) => {
                Object.keys(item.properties).forEach((key: string) => {
                    if (key !== 'createdAt') {
                        items.push(key)
                    }
                })
                if (item.userProperties) {
                    Object.keys(item.userProperties).forEach((key: string) => {
                        items.push(key)
                    })
                }
            })

            return [...new Set(items)]
        },
        isNoData(): boolean {
            return !this.reports.length && !this.loading
        },
    },
    actions: {
        toggleColumns(key: string) {
            if (this.activeColumns.includes(key)) {
                this.activeColumns = this.activeColumns.filter(item => item !== key)
            } else {
                this.activeColumns.push(key)
            }
        },
        async getReportLiveStream() {
            this.loading = true
            const commonStore = useCommonStore()

            const res = await dataService.createEventsStream(String(commonStore.projectId), {
                time: this.timeRequest,
                events: this.eventsRequest,
                filters: this.filtersRequest,
            })

            if (res?.data) {
                this.reports = res.data

                if (this.columnsMap) {
                    this.activeColumns = this.columnsMap
                }
            }

            this.loading = false
        },
    }
})
