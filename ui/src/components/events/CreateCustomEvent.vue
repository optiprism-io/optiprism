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
import { EventRef } from '@/types/events'
import { useLexiconStore } from '@/stores/lexicon'
import { Event, useEventsStore, EventPayload } from '@/stores/eventSegmentation/events'
import { useCommonStore } from '@/stores/common'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiInput from '@/components/uikit/UiInput.vue'
import UiFormLabel from '@/components//uikit/UiFormLabel.vue'
import Select from '@/components/Select/Select.vue'
import SelectedEvent from '@/components/events/Events/SelectedEvent.vue'
import schemaService from '@/api/services/schema.service'
import { CreateCustomEventRequest, CustomEventEvent } from '@/api'
const i18n = inject<any>('i18n')

const lexiconStore = useLexiconStore()
const eventsStore = useEventsStore()
const commonStore = useCommonStore()

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
            type: ref.type,
            id: ref.id
        },
        filters: [],
        breakdowns: [],
        queries: [],
    })
}

const setEvent = (payload: EventPayload) => {
    events.value[payload.index] = payload.event
}

const removeEvent = (idx: number): void => {
    events.value.splice(idx, 1)
}

const apply = async () => {
    loading.value = true
    try {
        const data: CreateCustomEventRequest = {
            name: eventName.value,
            events: events.value.map((item): CustomEventEvent => {
                const event = lexiconStore.findEventById(item.ref.id)

                const eventProps: CustomEventEvent = {
                    eventName: event.name,
                    eventType: 'custom',
                    eventId: event.id,
                    filters: [],
                }

                if (item.filters.length) {
                    item.filters.forEach(filter => {
                        if (filter.propRef) {
                            if (eventProps.filters) {
                                eventProps.filters.push({
                                    type: 'property',
                                    propertyType: 'custom',
                                    propertyId: filter.propRef.id,
                                    operation: filter.opId,
                                    value: filter.values,
                                })
                            }
                        }
                    })
                }

                return eventProps
            }),
        }

        if (isEdit.value) {
            await schemaService.updateCustomEvent(commonStore.organizationId, commonStore.projectId, String(editedEvent.value?.id), data)
        } else {
            await schemaService.createCustomEvent(commonStore.organizationId, commonStore.projectId, data)
        }


        await lexiconStore.getEvents()
    } catch(error: unknown) {
        loading.value = false
        throw Error(JSON.stringify(error))
    }

    loading.value = false
    emit('apply')
}

const cancel = () => {
    emit('cancel')
}

onBeforeMount(async () => {
    if (editedEvent.value) {
        eventName.value = editedEvent.value.name

        if (editedEvent.value.events) {
            events.value = JSON.parse(JSON.stringify(await Promise.all(editedEvent.value.events.map(async item => {
                return {
                    ref: {
                        type: 'regular',
                        id: item.eventId
                    },
                    filters: item.filters ? await Promise.all(item.filters.map(async filter => {
                        let valuesList: string[] = []

                        try {
                            const res = await schemaService.propertyValues({
                                event_name: item.eventName,
                                event_type: item.eventType,
                                property_name: filter.propertyName || '',
                                property_type: filter.propertyType,
                            })
                            if (res) {
                                valuesList = res
                            }
                        } catch (error) {
                            throw new Error('error get events values')
                        }

                        return {
                            propRef: {
                                type: filter.propertyType,
                                id: filter.propertyId
                            },
                            opId: filter.operation,
                            values: filter.value,
                            valuesList: valuesList,
                        }
                    })) : [],
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