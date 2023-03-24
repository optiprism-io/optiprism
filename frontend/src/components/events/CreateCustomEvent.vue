<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
        :title="title"
        :apply-disabled="applyDisabled"
        :apply-loading="loading"
        @apply="apply"
        @cancel="cancel"
    >
        <UiFormLabel
            class="pf-u-mb-lg"
            :text="$t('events.event_management.columns.name')"
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
        <UiFormLabel
            class="pf-u-mb-lg"
            :text="$t('events.event_management.columns.description')"
            :for="'eventDescription'"
        >
            <UiTextarea
                :value="eventDescription"
                :name="'eventDescription'"
                :label="$t('events.event_management.columns.description')"
                @input="inputTextarea"
            />
        </UiFormLabel>
        <UiFormLabel
            class="pf-u-mb-md"
            :text="$t('events.events')"
        >
            <div class="pf-l-flex pf-m-column pf-u-mb-md">
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
        </UiFormLabel>
        <UiFormLabel
            class="pf-u-mb-md"
            :text="$t('events.event_management.columns.tags')"
        >
            <UiInputTags
                :value="eventTags"
                @input="inputTags"
            />
        </UiFormLabel>
        <UiFormLabel
            v-if="isEdit"
            class="pf-u-mb-md"
            :text="$t('events.event_management.columns.status')"
        >
            <UiSwitch
                :value="eventStatus"
                @input="inputStatus"
            />
        </UiFormLabel>
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
import UiTextarea from '@/components/uikit/UiTextarea.vue'
import UiInputTags from '@/components/uikit/UiInputTags.vue'
import UiSwitch from '@/components/uikit/UiSwitch.vue'
import UiFormLabel from '@/components//uikit/UiFormLabel.vue'
import Select from '@/components/Select/Select.vue'
import SelectedEvent from '@/components/events/Events/SelectedEvent.vue'
import schemaService from '@/api/services/schema.service'
import {
    CreateCustomEventRequest,
    CustomEventEvent,
    EventType,
    EventStatus,
    UpdateCustomEventRequest,
    Value,
    CustomEventStatus,
} from '@/api'
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
const eventDescription = ref('')
const eventTags = ref<string[]>([])
const eventStatus = ref(true)

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
    return lexiconStore.eventsList.filter((group) => !group.type && group.type !== EventType.Custom)
})

const inputStatus = (payload: boolean) => {
    eventStatus.value = payload
}

const inputTags = (payload: string[]) => {
    eventTags.value = payload
}

const inputTextarea = (payload: string) => {
    eventDescription.value = payload
}

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

const resultEvent = computed(() => {
    return {
        name: eventName.value,
        description: eventDescription.value,
        tags: eventTags.value,
        status: CustomEventStatus.Enabled,
        isSystem: false,
        events: events.value.map((item): CustomEventEvent => {
            const event = lexiconStore.findEventById(item.ref.id)

            const eventProps: CustomEventEvent = {
                eventName: event.name,
                eventType: item.ref.type as EventType,
                eventId: event.id,
                filters: [],
            }

            if (item.filters.length) {
                item.filters.forEach(filter => {
                    if (filter.propRef) {
                        if (eventProps.filters) {
                            eventProps.filters.push({
                                type: 'property',
                                propertyType: filter.propRef.type,
                                propertyId: filter.propRef.id,
                                operation: filter.opId,
                                value: filter.values,
                            })
                        }
                    }
                })
            }

            return eventProps
        })
    }
})

const apply = async () => {
    loading.value = true
    try {
        if (isEdit.value) {
            const data: UpdateCustomEventRequest = resultEvent.value
            data.status = eventStatus.value ? EventStatus.Enabled : EventStatus.Disabled;
            await schemaService.updateCustomEvent(commonStore.organizationId, commonStore.projectId, String(editedEvent.value?.id), data)
        } else {
            const data: CreateCustomEventRequest = resultEvent.value
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
        eventDescription.value = editedEvent.value?.description || ''
        eventTags.value = editedEvent.value?.tags || []
        eventStatus.value = editedEvent.value?.status === EventStatus.Enabled

        if (editedEvent.value.events) {
            events.value = JSON.parse(JSON.stringify(await Promise.all(editedEvent.value.events.map(async item => {
                return {
                    ref: {
                        type: EventType.Regular,
                        id: item.eventId
                    },
                    filters: item.filters ? await Promise.all(item.filters.map(async filter => {
                        let valuesList: Array<Value> = []

                        try {
                            const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
                                eventName: item.eventName,
                                eventType: item.eventType,
                                propertyName: filter.propertyName || '',
                                propertyType: filter.propertyType,
                            })
                            if (res.data.data) {
                                valuesList = res.data.data
                            }
                        } catch (error) {
                            throw new Error('error get events values')
                        }

                        return {
                            propRef: {
                                type: filter.propertyType,
                                id: filter?.propertyId || 0,
                                name: filter?.propertyName || '',
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