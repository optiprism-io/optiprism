<template>
    <div class="queries pf-l-flex">
        <div class="pf-c-action-list">
            <div class="pf-c-action-list__item min-w-50 pf-u-text-align-right">
                query
            </div>
            <div class="pf-c-action-list__item">
                <Select
                    v-if="item.queryRef"
                    :items="lexiconStore.eventsQueries"
                    :selected="item.queryRef"
                    :width-auto="true"
                    @select="changeQuery"
                >
                    <UiButton class="pf-m-main pf-m-secondary">
                        {{ querySelectorName }}
                    </UiButton>
                </Select>
                <Select
                    v-else
                    :items="lexiconStore.eventsQueries"
                    :update-open="updateOpen"
                    @select="changeQuery"
                >
                    <UiButton
                        :before-icon="'fas fa-plus-circle'"
                        class="pf-m-main pf-m-primary"
                        type="button"
                    >
                        Select Query
                    </UiButton>
                </Select>
            </div>

            <PropertySelect
                v-if="showProperty"
                :event-ref="eventRef"
                :selected="propRef"
                :width-auto="true"
                @select="changeProperty"
            >
                <UiButton
                    class="pf-m-main"
                    :before-icon="!propRef ? 'fas fa-plus-circle' : ''"
                    :class="{
                        'pf-m-secondary': propRef,
                        'pf-m-primary': !propRef,
                    }"
                >
                    {{ propertyName }}
                </UiButton>
            </PropertySelect>
            <div
                v-if="showAggregateText"
                class="pf-c-action-list__item pf-u-text-align-right"
            >
                {{ aggregateString }}
            </div>
            <div
                v-if="showOnlyAggregate"
                class="pf-c-action-list__item"
            >
                <Select
                    :items="lexiconStore.eventsQueryAggregates"
                    :selected="selectedAggregateRef"
                    :show-search="false"
                    :width-auto="true"
                    @select="changeQueryAggregate"
                >
                    <UiButton
                        class="pf-m-main"
                        :before-icon="!selectedAggregateRef ? 'fas fa-plus-circle' : ''"
                        :class="{
                            'pf-m-secondary': selectedAggregateRef,
                            'pf-m-primary': !selectedAggregateRef,
                        }"
                    >
                        {{ selectAggregateName }}
                    </UiButton>
                </Select>
            </div>
            <div
                v-if="showGroupAggregate"
                class="pf-c-action-list__item"
            >
                <Select
                    :items="lexiconStore.eventsQueryAggregates"
                    :selected="selectedGroupAggregateRef"
                    :show-search="false"
                    :width-auto="true"
                    @select="changeQueryGroupAggregate"
                >
                    <UiButton
                        class="pf-m-main"
                        :before-icon="!selectedGroupAggregateRef ? 'fas fa-plus-circle' : ''"
                        :class="{
                            'pf-m-secondary': selectedGroupAggregateRef,
                            'pf-m-primary': !selectedGroupAggregateRef,
                        }"
                    >
                        {{ selectedGroupAggregateName }}
                    </UiButton>
                </Select>
            </div>
            <div
                v-if="queryInfo?.hasValue"
                class="pf-c-action-list__item"
            >
                <UiInput @input="changeFormula" />
            </div>
            <div
                v-if="!noDelete"
                class="pf-c-action-list__item queries__control-item"
                @click="removeQuery"
            >
                <VTooltip popper-class="ui-hint">
                    <UiIcon icon="fas fa-times" />
                    <template #popper>
                        Remove query
                    </template>
                </VTooltip>
            </div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { EventQuery } from '@/stores/eventSegmentation/events';
import { useLexiconStore } from '@/stores/lexicon';
import { useEventsStore, Events } from '@/stores/eventSegmentation/events';
import { EventRef, EventQueryRef, EventsQuery, PropertyRef } from '@/types/events';
import { AggregateRef } from '@/types/aggregate';
import { Item } from '@/components/Select/SelectTypes';
import UiInput from '@/components/uikit/UiInput.vue';
import Select from '@/components/Select/Select.vue';
import PropertySelect from '@/components/events/PropertySelect.vue';
import UiButton from '@/components/uikit/UiButton.vue';

const eventsStore: Events = useEventsStore();
const lexiconStore = useLexiconStore();

const props = defineProps<{
    eventRef: EventRef;
    item: EventQuery;
    index: number;
    updateOpen?: boolean;
    noDelete?: boolean;
}>();

const emit = defineEmits<{
    (e: 'removeQuery', index: number): void;
    (e: 'changeQuery', queryIdx: number, queryRef: EventQueryRef): void;
}>();

const queryInfo = computed((): EventsQuery | undefined => {
    return lexiconStore.findQuery(props.item.queryRef);
});

const propRef = computed((): PropertyRef | undefined => {
    return props?.item?.queryRef?.propRef;
});

const showProperty = computed(() => {
    return queryInfo.value?.hasProperty;
});

const propertyName = computed((): string => {
    return props.item?.queryRef?.propRef ? lexiconStore.propertyName(props.item?.queryRef?.propRef): 'Select property';
});

const showOnlyAggregate = computed(() => {
    return queryInfo.value?.hasAggregate && !queryInfo.value?.hasProperty;
});

const showGroupAggregate = computed(() => {
    return queryInfo.value?.hasGroupAggregate;
});

const showAggregateText = computed(() => {
    return showGroupAggregate.value || showOnlyAggregate.value;
});

const selectedAggregate = computed((): Item<EventQueryRef, null> | undefined => {
    return props.item?.queryRef?.typeAggregate ? lexiconStore.eventsQueryAggregates.find(item => props.item?.queryRef?.typeAggregate === item.item.typeAggregate) : undefined;
});

const selectAggregateName = computed((): string => {
    return selectedAggregate.value?.name || 'Select agregate';
});

const selectedAggregateRef = computed((): AggregateRef | undefined => {
    return selectedAggregate.value?.item as AggregateRef || undefined;
});

const selectedGroupAggregate = computed((): Item<EventQueryRef, null> | undefined => {
    return props.item?.queryRef?.typeGroupAggregate ?
        lexiconStore.eventsQueryAggregates.find(item => props.item?.queryRef?.typeGroupAggregate === item.item.typeAggregate) :
        undefined;
});

const selectedGroupAggregateRef = computed((): AggregateRef | undefined => {
    return selectedGroupAggregate.value?.item as AggregateRef || undefined;
});

const selectedGroupAggregateName = computed((): string => {
    return selectedGroupAggregate.value?.name || 'Select agregate';
});

const querySelectorName = computed((): string => {
    return showProperty.value && props.item.queryRef ? `${selectAggregateName.value} of property` : queryInfo.value?.displayName || '';
});

const aggregateString = computed((): string => {
    return `per ${eventsStore.group}, agregate by`;
});

const changeQueryAggregate = (payload: AggregateRef) => {
    if (props.item.queryRef) {
        emit('changeQuery', props.index, {
            ...props.item.queryRef,
            ...payload
        });
    }
};

const changeQueryGroupAggregate = (payload: AggregateRef) => {
    if (props.item.queryRef) {
        emit('changeQuery', props.index, {
            ...props.item.queryRef,
            typeGroupAggregate: payload.typeAggregate,
        });
    }
};

const changeQuery = (payload: EventQueryRef) => {
    emit('changeQuery', props.index, payload);
}

const removeQuery = () => {
    emit('removeQuery', props.index);
}

const changeProperty = (payload: PropertyRef) => {
    if (props.item.queryRef) {
        emit('changeQuery', props.index, {
            ...props.item.queryRef,
            propRef: payload,
        });
    }
};

const changeFormula = (value: string) => {
    if (props.item.queryRef) {
        emit('changeQuery', props.index, {
            ...props.item.queryRef,
            value,
        })
    }
}
</script>

<style scoped lang="scss">
.queries {
    &:hover {
        .queries__control-item {
            opacity: 1;
        }
    }

    &__control-item {
        opacity: 0;
        cursor: pointer;
    }
}
</style>
