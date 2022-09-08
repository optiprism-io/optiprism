<template>
    <div class="breakdown pf-l-flex">
        <div class="pf-c-action-list">
            <AlphabetIdentifier
                v-if="showIdentifier"
                :index="index"
            />
            <div
                v-else
                class="pf-c-action-list__item min-w-50 pf-u-text-align-right"
            >
                group
            </div>
            <div class="pf-c-action-list__item">
                <PropertySelect
                    v-if="breakdown.propRef"
                    :event-ref="eventRef"
                    :selected="breakdown.propRef"
                    :disabled-items="selectedItems"
                    @select="changeProperty"
                >
                    <UiButton class="pf-m-main pf-m-secondary">
                        {{ propertyName(breakdown.propRef) }}
                    </UiButton>
                </PropertySelect>
                <PropertySelect
                    v-else
                    :is-open-mount="true"
                    :event-ref="eventRef"
                    :update-open="updateOpen"
                    :disabled-items="selectedItems"
                    @select="changeProperty"
                >
                    <UiButton
                        :before-icon="'fas fa-plus-circle'"
                        class="pf-m-main pf-m-primary"
                        type="button"
                        @click="handleSelectProperty"
                    >
                        Select Breakdown
                    </UiButton>
                </PropertySelect>
            </div>
            <div
                v-if="breakdown.error"
                class="pf-c-action-list__item"
            >
                <VTooltip popper-class="ui-hint">
                    <UiIcon
                        class="pf-u-warning-color-100"
                        icon="fas fa-exclamation-triangle"
                    />
                    <template #popper>
                        This breakdown will not work because no event was found for the selected property
                    </template>
                </VTooltip>
            </div>
            <div class="pf-c-action-list__item breakdown__control-item">
                <UiButton
                    class="pf-m-plain"
                    icon="fas fa-times"
                    @click="removeBreakdown"
                />
            </div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { EventBreakdown } from '@/stores/eventSegmentation/events';
import { useLexiconStore } from '@/stores/lexicon';
import PropertySelect from '@/components/events/PropertySelect.vue';
import { EventRef, PropertyRef } from '@/types/events';
import UiButton from '@/components/uikit/UiButton.vue';
import AlphabetIdentifier from '@/components/common/identifier/AlphabetIdentifier.vue';
import { PropertyType } from '@/api'

const lexiconStore = useLexiconStore();
const props = defineProps<{
    eventRef?: EventRef;
    eventRefs?: EventRef[];
    breakdown: EventBreakdown;
    index: number;
    updateOpen?: boolean;
    selectedItems?: EventBreakdown[];
    showIdentifier?: boolean;
}>();

const emit = defineEmits<{
    (e: 'removeBreakdown', index: number): void;
    (e: 'changeBreakdownProperty', breakdownIdx: number, propRef: PropertyRef): void;
    (e: 'handleSelectProperty'): void;
}>();

const removeBreakdown = (): void => {
    emit('removeBreakdown', props.index);
};

const changeProperty = (propRef: PropertyRef): void => {
    emit('changeBreakdownProperty', props.index, propRef);
};

const handleSelectProperty = (): void => {
    emit('handleSelectProperty');
};

const propertyName = (ref: PropertyRef): string => {
    switch (ref.type) {
        case PropertyType.Event:
            return lexiconStore.findEventPropertyById(ref.id).name
        case PropertyType.Custom:
            return lexiconStore.findEventCustomPropertyById(ref.id)?.name || ''
        case PropertyType.User:
            return lexiconStore.findUserPropertyById(ref.id).name
    }
    throw new Error('unhandled');
};
</script>

<style scoped lang="scss">
.breakdown {
    &:hover {
        .breakdown__control-item {
            opacity: 1;
        }
    }

    &__control-item {
        opacity: 0;
    }
}
</style>
