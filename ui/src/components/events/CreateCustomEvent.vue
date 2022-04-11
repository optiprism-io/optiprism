<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
        :title="title"
        :description="$t('events.create_custom_description')"
        :apply-disabled="applyDisabled"
        :apply-loading="loading"
        @apply="apply"
        @cancel="cancel"
    >
        <UiFormLabel
            class="pf-u-mb-lg"
            :text="$t('events.custom_event_name')"
            :required="true"
            :for="'eventName'"
        >
            <UiInput
                v-model="eventName"
                :required="true"
                :name="'eventName'"
                :label="$t('events.custom_event_name')"
            />
        </UiFormLabel>
        <div class="pf-u-font-size-md pf-u-font-weight-bold pf-u-mb-md">
            {{ $t('events.events') }}
        </div>
        <div class="pf-l-flex pf-m-column">
            <SelectedEvent
                v-for="(event, index) in events"
                :key="index"
                :event="event"
                :event-ref="event.ref"
                :filters="event.filters"
                :index="index"
                :event-items="eventItems"
                :show-breakdowns="false"
                :show-query="false"
                :popper-container="'.ui-popup-window__box'"
                @set-event="setEvent"
                @remove-event="removeEvent"
            />
        </div>
        <Select
            grouped
            :items="eventItems"
            :width-auto="true"
            :popper-class="'popup-floating-popper'"
            :popper-container="'.ui-popup-window__box'"
            @select="addEvent"
        >
            <UiButton
                class="pf-m-main"
                :is-link="true"
                :before-icon="'fas fa-plus'"
            >
                {{ $t('common.add_event') }}
            </UiButton>
        </Select>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { ref, computed, onBeforeMount, inject } from 'vue'
import { EventRef, EVENT_TYPE_REGULAR, PropertyType } from '@/types/events'
import { useLexiconStore } from '@/stores/lexicon'
import { useEventsStore } from '@/stores/eventSegmentation/events'
import { Event } from '@/stores/eventSegmentation/events'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiInput from '@/components/uikit/UiInput.vue'
import UiFormLabel from '@/components//uikit/UiFormLabel.vue'
import Select from '@/components/Select/Select.vue'
import SelectedEvent, { SetEventPayload } from '@/components/events/Events/SelectedEvent.vue'
import schemaService, { Event as EventScheme, CustomEvents } from '@/api/services/schema.service'

const i18n = inject<any>('i18n')

const lexiconStore = useLexiconStore()
const eventsStore = useEventsStore()

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()

const loading = ref(false)
const eventName = ref('')
const events = ref<Event[]>([])

const applyDisabled = computed(() => {
    return !(events.value.length && Boolean(eventName.value))
})

const isEdit = computed(() => {
    return eventsStore.editCustomEvent
})

const editedEvent = computed(() => {
    if (eventsStore.editCustomEvent) {
        return lexiconStore.findCustomEventById(eventsStore.editCustomEvent)
    } else {
        return null
    }
})

const title = computed(() => {
    return isEdit.value ? i18n.$t('events.edit_custom') : i18n.$t('events.create_custom')
})

const eventItems = computed(() => {
    return lexiconStore.eventsList.filter((group) => !group.type && group.type !== 'custom')
})

const addEvent = (ref: EventRef) => {
    events.value.push({
        ref: {
            type: EVENT_TYPE_REGULAR,
            id: ref.id
        },
        filters: [],
        breakdowns: [],
        queries: [],
    })
}

const setEvent = (payload: SetEventPayload) => {
    events.value[payload.index] = payload.event
}

const removeEvent = (idx: number): void => {
    events.value.splice(idx, 1)
}

const apply = async () => {
    loading.value = true
    try {

        const params: CustomEvents = {
            name: eventName.value,
            events: events.value.map(item => {
                const event = lexiconStore.findEventById(item.ref.id)

                const eventProps: EventScheme = {
                    eventName: event.name,
                    eventType: item.ref.type,
                    filters: [],
                    ref: item.ref,
                }

                if (item.filters.length) {
                    item.filters.forEach(filter => {
                        if (filter.propRef) {
                            const property = lexiconStore.findEventPropertyById(filter.propRef.id)

                            if (eventProps.filters) {
                                eventProps.filters.push({
                                    filterType: 'property',
                                    propertyName: property.name,
                                    propertyType: PropertyType[filter.propRef.type],
                                    operation: filter.opId,
                                    value: filter.values,
                                    propRef: filter.propRef,
                                    valuesList: filter.valuesList
                                })
                            }
                        }
                    })
                }

                return eventProps
            }),
        }

        if (isEdit.value) {
            params.id = editedEvent.value?.id

            await schemaService.editCustomEvents(params)
        } else {
            await schemaService.createCustomEvents(params)
        }


        await lexiconStore.getEvents()
    } catch {
        loading.value = false
        throw new Error('create custom events error');
    }

    loading.value = false
    emit('apply')
}

const cancel = (type: string) => {
    emit('cancel')
}

onBeforeMount(() => {
    if (editedEvent.value) {
        eventName.value = editedEvent.value.name

        if (editedEvent.value.events) {
            events.value = JSON.parse(JSON.stringify((editedEvent.value.events.map(item => {
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
            }))))
        }
    }
})
</script>

<style lang="scss">
</style>