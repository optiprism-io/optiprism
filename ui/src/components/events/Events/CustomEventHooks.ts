import { ref, computed } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { EventRef } from '@/types/events'

export default function useCustomEvent(){
    const lexiconStore = useLexiconStore()
    const hoveredCustomEventId = ref<number | null>()

    const editedEvent = computed(() => {
        if (hoveredCustomEventId.value) {
            return lexiconStore.findCustomEventById(hoveredCustomEventId.value)
        } else {
            return null
        }
    })

    const hoveredCustomEventDescription = computed(() => {
        if (editedEvent.value && editedEvent.value.events) {
            return editedEvent.value.events.map(item => {
                return {
                    ref: item.ref,
                    filters: item.filters ? item.filters.map(filter => {
                        return {
                            propRef: filter.propRef,
                            opId: filter.operation,
                            values: filter.value,
                            valuesList: filter.valuesList || [],
                        }
                    }) : [],
                    breakdowns: [],
                    queries: [],
                }
            })
        } else {
            return null
        }
    })

    const onHoverEvent = (payload: EventRef) => {
        if (payload.type === 'custom') {
            hoveredCustomEventId.value = payload.id
        } else {
            hoveredCustomEventId.value = null
        }
    }

    return {
        editedEvent,
        hoveredCustomEventDescription,
        hoveredCustomEventId,
        onHoverEvent
    }
}