<template>
    <Select
        grouped
        :items="lexiconStore.eventsList"
        :width-auto="true"
        :auto-hide="!commonStore.showCreateCustomEvent"
        @action="selectAction"
        @select="emit('select', $event)"
        @edit="editEvent"
        @on-hover="onHoverEvent"
    >
        <slot>
            <UiButton
                class="pf-m-main"
                :is-link="true"
                :before-icon="'fas fa-plus'"
            >
                {{ $t('common.add_event') }}
            </UiButton>
        </slot>
        <template
            v-if="hoveredCustomEventId"
            #description
        >
            <div class="pf-l-flex pf-m-column">
                <SelectedEvent
                    v-for="(event, index) in hoveredCustomEventDescription"
                    :key="index"
                    :event="event"
                    :event-ref="event.ref"
                    :filters="event.filters"
                    :index="index"
                    :show-breakdowns="false"
                    :show-query="false"
                    :for-preview="true"
                />
            </div>
        </template>
    </Select>
</template>

<script setup lang="ts">
import Select from '@/components/Select/Select.vue';
import SelectedEvent from '@/components/events/Events/SelectedEvent.vue';
import {useLexiconStore} from '@/stores/lexicon';
import {useCommonStore} from '@/stores/common';
import {useEventsStore} from '@/stores/eventSegmentation/events';
import useCustomEvent from '@/components/events/Events/CustomEventHooks';

const lexiconStore = useLexiconStore();
const commonStore = useCommonStore();
const eventsStore = useEventsStore();

const emit = defineEmits(['select'])

const { hoveredCustomEventDescription, hoveredCustomEventId, onHoverEvent } = useCustomEvent()

const selectAction = (payload: string) => {
    if (payload === 'createCustomEvent') {
        commonStore.togglePopupCreateCustomEvent(true)
    }
}

const editEvent = (payload: number) => {
    eventsStore.setEditCustomEvent(Number(payload))
    commonStore.togglePopupCreateCustomEvent(true)
}
</script>
