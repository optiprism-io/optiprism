<template>
    <div class="pf-l-flex">
        <CommonIdentifier
            class="pf-l-flex__item"
            type="numeric"
            :index="index"
        />
        <div class="pf-l-flex pf-m-column">
            <div
                v-for="(event, i) in step.events"
                :key="event.event.id"
                class="pf-l-flex pf-m-column"
            >
                <UiActionList>
                    <template #main>
                        <EventSelector @select="(value) => editStepEvent(i, value)">
                            <UiButton class="pf-m-main pf-m-secondary">
                                {{ eventName(event.event) }}
                            </UiButton>
                        </EventSelector>
                    </template>

                    <UiActionListItem>
                        <VTooltip class="ui-hint">
                            <EventSelector @select="(value) => addStepToEvent(index, value)">
                                <UiIcon icon="fas fa-plus" />
                            </EventSelector>
                            <template #popper>
                                {{ $t('funnels.steps.addEvent') }}
                            </template>
                        </VTooltip>
                    </UiActionListItem>

                    <UiActionListItem>
                        <VTooltip class="ui-hint">
                            <UiIcon
                                icon="fas fa-filter"
                                @click="addFilterToStep(i)"
                            />
                            <template #popper>
                                {{ $t('funnels.steps.addFilter') }}
                            </template>
                        </VTooltip>
                    </UiActionListItem>

                    <UiActionListItem>
                        <VTooltip class="ui-hint">
                            <UiIcon
                                icon="fas fa-trash"
                                @click="deleteEventFromStep(i)"
                            />
                            <template #popper>
                                {{ $t('funnels.steps.removeEvent') }}
                            </template>
                        </VTooltip>
                    </UiActionListItem>
                </UiActionList>

                <Filter
                    v-for="(filter, idx) in event.filters"
                    :key="idx"
                    :filter="filter"
                    :event-ref="event.event"
                    :index="idx"
                    @remove-filter="removeFilterForStepEvent(i, idx)"
                    @change-filter-property="(...args) => changeFilterPropertyForStepEvent(i, ...args)"
                    @change-filter-operation="(...args) => changeFilterOperationForEvent(i, ...args)"
                    @add-filter-value="(...args) => addFilterValueForStepEvent(i, ...args)"
                    @remove-filter-value="(...args) => removeFilterValueForStepEvent(i, ...args)"
                />
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import {useStepsStore} from '@/stores/funnels/steps';
import CommonIdentifier from '@/components/common/identifier/CommonIdentifier.vue';
import {PropType, watch} from 'vue';
import {Step} from '@/types/steps';
import {useEventsStore} from '@/stores/eventSegmentation/events';
import UiActionList from '@/components/uikit/UiActionList/UiActionList.vue';
import {useEventName} from '@/helpers/useEventName';
import {EventRef, PropertyRef} from '@/types/events';
import {useLexiconStore} from '@/stores/lexicon';
import EventSelector from '@/components/events/Events/EventSelector.vue';
import UiActionListItem from '@/components/uikit/UiActionList/UiActionListItem.vue';
import {OperationId, Value} from '@/types';
import Filter from '@/components/events/Filter.vue';
import schemaService from '@/api/services/schema.service';
import {useFilter} from '@/hooks/useFilter';

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();
const stepsStore = useStepsStore();
const eventName = useEventName()
const { getValues } = useFilter()

const props = defineProps({
    index: {
        type: Number,
        required: true,
    },
    step: {
        type: Object as PropType<Step>,
        required: true,
    }
})

const addStepToEvent = (index: number, event: EventRef): void => {
    stepsStore.addEventToStep({index, event})
}

const editStepEvent = (eventIndex: number, eventRef: EventRef): void => {
    stepsStore.editStepEvent({
        index: props.index,
        eventIndex,
        eventRef,
    });
}

const deleteEventFromStep = (eventIndex: number): void => {
    stepsStore.deleteEventFromStep({
        index: props.index,
        eventIndex
    });
}

const addFilterToStep = (eventIndex: number): void => {
    stepsStore.addFilterToStep({
        index: props.index,
        eventIndex,
        filter: {
            opId: OperationId.Eq,
            values: [],
            valuesList: []
        }
    });
}

const removeFilterForStepEvent = (eventIndex: number, filterIndex: number): void => {
    stepsStore.removeFilterForStepEvent({
        index: props.index,
        eventIndex,
        filterIndex,
    })
}

const changeFilterPropertyForStepEvent = async (eventIndex: number, filterIndex: number, payload: PropertyRef): Promise<void> => {
    stepsStore.editFilterForStepEvent({
        index: props.index,
        eventIndex,
        filterIndex,
        filter: {
            propRef: payload,
            valuesList: await getValues(payload)
        }
    })
}

const addFilterValueForStepEvent = (eventIndex: number, filterIndex: number, payload: Value): void => {
    stepsStore.editFilterForStepEvent({
        index: props.index,
        eventIndex,
        filterIndex,
        filter: {
            values: [
                ...props.step.events[eventIndex].filters[filterIndex].values,
                payload
            ]
        }
    })
}

const changeFilterOperationForEvent = (eventIndex: number, filterIndex: number, payload: OperationId): void => {
    stepsStore.editFilterForStepEvent({
        index: props.index,
        eventIndex,
        filterIndex,
        filter: {
            opId: payload
        }
    })
}

const removeFilterValueForStepEvent = (eventIndex: number, filterIndex: number, value: Value): void => {
    stepsStore.editFilterForStepEvent({
        index: props.index,
        eventIndex,
        filterIndex,
        filter: {
            values: props.step.events[eventIndex]
                .filters[filterIndex]
                .values.filter(item => item !== value)
        }
    })
}

watch(() => props.step.events.length, (value): void => {
    if (value === 0) {
        stepsStore.deleteStep(props.index);
    }
})
</script>
