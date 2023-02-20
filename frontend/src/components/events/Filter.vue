<template>
    <div class="filter pf-l-flex">
        <div class="pf-c-action-list">
            <AlphabetIdentifier
                v-if="showIdentifier"
                :index="index"
            />
            <div
                v-else
                class="pf-c-action-list__item min-w-50 pf-u-text-align-right"
            >
                <slot
                    v-if="!hidePrefix"
                    name="prefix"
                >
                    with
                </slot>
            </div>
            <div class="pf-c-action-list__item">
                <PropertySelect
                    v-if="filter.propRef"
                    :event-ref="eventRef"
                    :event-refs="eventRefs"
                    :selected="filter.propRef"
                    :popper-container="props.popperContainer"
                    @select="changeProperty"
                >
                    <UiButton
                        :class="[props.forPreview ? 'pf-m-control pf-m-small' : 'pf-m-secondary']"
                        :disabled="props.forPreview"
                    >
                        {{ filter.propRef?.name || propertyName(filter.propRef) }}
                    </UiButton>
                </PropertySelect>
                <PropertySelect
                    v-else
                    :is-open-mount="true"
                    :event-ref="eventRef"
                    :update-open="updateOpen"
                    :popper-container="props.popperContainer"
                    @select="changeProperty"
                >
                    <UiButton
                        :before-icon="'fas fa-plus-circle'"
                        class="pf-m-main pf-m-primary"
                        type="button"
                        @click="handleSelectProperty"
                    >
                        Select property
                    </UiButton>
                </PropertySelect>
            </div>

            <div
                v-if="isShowOperation && filter.propRef"
                class="pf-c-action-list__item"
            >
                <OperationSelect
                    :property-ref="filter.propRef"
                    :selected="filter.opId"
                    :popper-container="props.popperContainer"
                    @select="changeOperation"
                >
                    <UiButton
                        :class="[props.forPreview ? 'pf-m-control pf-m-small' : 'pf-m-secondary']"
                        :disabled="props.forPreview"
                    >
                        {{ operationButtonText }}
                    </UiButton>
                </OperationSelect>
            </div>

            <div
                v-if="isShowValues && filter.propRef"
                class="pf-c-action-list__item"
            >
                <ValueSelect
                    :property-ref="filter.propRef"
                    :selected="filter.values"
                    :items="filterItemValues"
                    :popper-container="props.popperContainer"
                    @add="addValue"
                    @deselect="removeValue"
                >
                    <template v-if="filter.values.length > 0">
                        <div class="pf-c-action-list">
                            <div
                                v-for="(value, i) in filter.values"
                                :key="i"
                                class="pf-c-action-list__item"
                            >
                                <UiButton
                                    :class="[props.forPreview ? 'pf-m-control pf-m-small' : 'pf-m-secondary']"
                                    :disabled="props.forPreview"
                                >
                                    {{ value }}

                                    <span
                                        v-if="!props.forPreview"
                                        class="pf-c-button__icon pf-m-end"
                                    >
                                        <UiIcon
                                            icon="fas fa-times"
                                            @click.stop="removeValueButton(value)"
                                        />
                                    </span>
                                </UiButton>
                            </div>
                        </div>
                    </template>
                    <template v-else>
                        <UiButton
                            class="pf-m-main"
                            :before-icon="'fas fa-plus-circle'"
                        >
                            Select value
                        </UiButton>
                    </template>
                </ValueSelect>
            </div>

            <div
                v-if="filter.error"
                class="pf-c-action-list__item"
            >
                <VTooltip popper-class="ui-hint">
                    <UiIcon
                        class="pf-u-warning-color-100"
                        icon="fas fa-exclamation-triangle"
                    />
                    <template #popper>
                        This filter will not work because no event was found for the selected property
                    </template>
                </VTooltip>
            </div>
            <div class="pf-c-action-list__item filter__control-item">
                <UiButton
                    class="pf-m-plain"
                    icon="fas fa-times"
                    @click="removeFilter"
                />
            </div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { EventFilter } from '@/stores/eventSegmentation/events';
import { useLexiconStore } from '@/stores/lexicon';
import PropertySelect from '@/components/events/PropertySelect.vue';
import OperationSelect from '@/components/events/OperationSelect.vue';
import ValueSelect from '@/components/events/ValueSelect.vue';
import { EventRef, PropertyRef } from '@/types/events';
import { operationById, OperationId, Value } from '@/types';
import AlphabetIdentifier from '@/components/common/identifier/AlphabetIdentifier.vue';
import { PropertyType } from '@/api'

const lexiconStore = useLexiconStore();

const props = defineProps<{
    eventRef?: EventRef;
    eventRefs?: EventRef[];
    filter: EventFilter;
    index: number;
    updateOpen?: boolean;
    showIdentifier?: boolean;
    popperContainer?: string;
    forPreview?: boolean;
    hidePrefix?: boolean;
}>();

const emit = defineEmits<{
    (e: 'removeFilter', index: number): void;
    (e: 'changeFilterProperty', filterIdx: number, propRef: PropertyRef): void;
    (e: 'changeFilterOperation', filterIdx: number, opId: OperationId): void;
    (e: 'addFilterValue', filterIdx: number, value: Value): void;
    (e: 'removeFilterValue', filterIdx: number, value: Value): void;
    (e: 'handleSelectProperty'): void;
}>();

const operationButtonText = computed(() => {
    return props.filter.opId ? operationById?.get(props.filter.opId)?.shortName || operationById?.get(props.filter.opId)?.name : '';
})

const filterItemValues = computed(() =>
    props.filter.valuesList.map((item: any) => {
        return { item, name: item };
    })
);

const isShowOperation = computed(() => {
    return !(props.forPreview && !props.filter.values.length)
})

const isShowValues = computed(() => {
    return !['exists', 'empty'].includes(props.filter.opId) && isShowOperation.value
})

const removeFilter = (): void => {
    emit('removeFilter', props.index);
};

const changeProperty = (propRef: PropertyRef): void => {
    emit('changeFilterProperty', props.index, propRef);
};

const handleSelectProperty = (): void => {
    emit('handleSelectProperty');
};

const changeOperation = (opId: OperationId): void => {
    emit('changeFilterOperation', props.index, opId);
};

const addValue = (value: Value): void => {
    emit('addFilterValue', props.index, value);
};

const removeValue = (value: Value) => {
    emit('removeFilterValue', props.index, value);
};

const removeValueButton = (value: Value) => {
    emit('removeFilterValue', props.index, value);
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
};
</script>

<style scoped lang="scss">
.filter {
    &:hover {
        .filter__control-item {
            opacity: 1;
        }
    }

    &__control-item {
        opacity: 0;
    }
}
</style>
