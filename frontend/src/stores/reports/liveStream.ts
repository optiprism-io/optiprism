import {defineStore} from 'pinia'
import {
    EventFilterByProperty,
    EventRecordRequestEvent,
    EventType,
    TimeBetween,
    TimeFrom,
    TimeLast,
    PropertyFilterOperation,
} from '@/api'
import {Event} from '@/stores/eventSegmentation/events'
import dataService from '@/api/services/datas.service'
import {useCommonStore} from '@/stores/common'
import {useLexiconStore} from '@/stores/lexicon'

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
    eventPopup: boolean
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
        defaultColumns: ['eventName', 'customEvents', 'createdAt'],
        loading: false,
        eventPopup: false
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
                                type: 'property',
                                propertyType: 'custom',
                                propertyId: filter.propRef.id,
                                operation: filter.opId as PropertyFilterOperation,
                                value: filter.values,
                            })
                        }
                    })
                }
            })

            return filters
        },
        eventsRequest(): Array<EventRecordRequestEvent> {
            const lexiconStore = useLexiconStore()

            return this.events.map(event => {
                const eventStore = lexiconStore.findEvent(event.ref)

                switch (event.ref.type) {
                    case EventType.Regular:
                        return {
                            eventName: eventStore.name,
                            eventType: event.ref.type as EventType,
                            filters: this.filtersRequest,
                        }
                    case EventType.Custom:
                        return {
                            eventId: eventStore.id,
                            eventType: event.ref.type,
                            filters: this.filtersRequest,
                        }
                }
            })
        },
        timeRequest(): TimeBetween | TimeFrom | TimeLast {
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
        columnsMapObject() {
            const properties: string[] = []
            const userProperties: string[] = []

            this.reports.forEach((item: Report) => {
                Object.keys(item.properties).forEach((key: string) => {
                    if (key !== 'createdAt') {
                        properties.push(key)
                    }
                })
                if (item.userProperties) {
                    Object.keys(item.userProperties).forEach((key: string) => {
                        userProperties.push(key)
                    })
                }
            })

            return {
                properties: [...new Set(properties)],
                userProperties: [...new  Set(userProperties)]
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

            try {
                const res = await dataService.createEventsStream(commonStore.organizationId, commonStore.projectId, {
                    time: this.timeRequest,
                    events: this.eventsRequest,
                })
                if (res?.data?.events) {
                    this.reports = res.data.events
                    if (this.columnsMap) {
                        this.activeColumns = this.columnsMap
                    }
                }
            } catch (error) {
                throw new Error('error get report live stream');
            }

            this.loading = false
        },
    }
})
