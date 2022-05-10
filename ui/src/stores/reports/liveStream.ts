import { defineStore } from 'pinia'
import { EventType, EventFilterByProperty, TimeBetween, TimeFrom, TimeLast, EventRef } from '@/api'
import { Event } from '@/stores/eventSegmentation/events'
import dataService from '@/api/services/datas.service'
import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'

type LiveStream = {
    events: Event[],
    controlsPeriod: string
    period: {
        from: string
        to: string
        last: number
        type: string
    },
    report: any // TODO integrations backend
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
        report: [],
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
    },
    actions: {
        async getReportLiveStream() {
            const commonStore = useCommonStore()

            const res = await dataService.createEventsStream(String(commonStore.projectId), {
                time: this.timeRequest,
                events: this.eventsRequest,
                filters: this.filtersRequest,
            })

            if (res?.data) {
                this.report = res.data
            }
        },
    }
})
