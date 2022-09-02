import { useEventsStore, Event } from '@/stores/eventSegmentation/events'
import { useStepsStore } from '@/stores/funnels/steps'
import { Step } from '@/types/steps'
import { EventRef } from '@/types/events'
import { Filter } from '@/types/filters'

export const funnelsToEvents = () => {
    const eventsStore = useEventsStore()
    const stepsStore = useStepsStore()

    eventsStore.events = stepsStore.steps.reduce((items: Event[], step) => {
        step.events.forEach(stepEvent => {
            items.push({
                ref: stepEvent.event,
                filters: stepEvent.filters,
                breakdowns: [],
                queries: [{
                    noDelete: true,
                    queryRef: {
                        name: 'countEvents',
                        type: 'simple'
                    }
                }]
            })
        })

        return items
    }, [])
}

export const eventsToFunnels = () => {
    const eventsStore = useEventsStore()
    const stepsStore = useStepsStore()

    stepsStore.steps = eventsStore.events.map((event): Step => {
        return {
            events: [{
                event: event.ref as EventRef,
                filters: event.filters as Filter[],
            }]
        }
    })
}