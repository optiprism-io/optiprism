<template>
    <div
        v-if="filterGroup"
        class="pf-l-flex pf-m-column"
    >
        <UiActionList>
            <template #main>
                <div class="pf-l-flex">
                    <span class="pf-l-flex__item">match</span>

                    <UiSelectMatch
                        :items="conditionsItems"
                        :show-search="false"
                        :value="filterGroup.condition"
                        @update:model-value="changeFilterGroupCondition"
                    >
                        <UiButton
                            class="pf-m-main pf-m-secondary pf-l-flex__item"
                            :is-link="true"
                        >
                            {{ $t(`filters.conditions.${filterGroup.condition}`) }}
                        </UiButton>
                    </UiSelectMatch>

                    <span class="pf-l-flex__item">in group</span>
                </div>
            </template>

            <UiActionListItem @click="removeFilterGroup">
                <VTooltip popper-class="ui-hint">
                    <UiIcon icon="fas fa-trash" />
                    <template #popper>
                        Remove group
                    </template>
                </VTooltip>
            </UiActionListItem>
        </UiActionList>

        <div class="pf-l-flex pf-m-column">
            <Filter
                v-for="(filter, i) in filterGroup.filters"
                :key="i"
                :index="i"
                :filter="filter"
                :event-refs="eventRefs"
                :hide-prefix="i === 0"
                @remove-filter="removeFilterForGroup(i)"
                @change-filter-property="changeFilterPropertyForGroup"
                @change-filter-operation="changeFilterOperationForGroup"
                @add-filter-value="addFilterValueForGroup"
                @remove-filter-value="removeFilterValueForGroup"
            >
                <template #prefix>
                    {{ filterGroup.condition }}
                </template>
            </Filter>
            <div class="pf-l-flex">
                <UiButton
                    class="pf-m-main"
                    :is-link="true"
                    :before-icon="'fas fa-plus'"
                    @click="addFilterToGroup"
                >
                    Add filter
                </UiButton>
                <UiButton
                    v-if="index === filterGroupsStore.filterGroups.length - 1"
                    class="pf-m-main"
                    :is-link="true"
                    :before-icon="'fas fa-plus'"
                    @click="filterGroupsStore.addFilterGroup"
                >
                    Add group
                </UiButton>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, inject} from 'vue';
import UiActionList from '@/components/uikit/UiActionList/UiActionList.vue';
import UiActionListItem from '@/components/uikit/UiActionList/UiActionListItem.vue';
import Filter from '@/components/events/Filter.vue';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';
import {OperationId, Value} from '@/types';
import {PropertyRef} from '@/types/events';
import {useStepsStore} from '@/stores/funnels/steps';
import { FilterCondition, filterConditions, FilterGroup, useFilterGroupsStore } from '@/stores/reports/filters'
import {useFilter} from '@/hooks/useFilter';
import {I18N} from '@/utils/i18n';

const UiSelectMatch = UiSelectGeneric<FilterCondition>();

const filterGroupsStore = useFilterGroupsStore()
const stepsStore = useStepsStore()
const filterHelpers = useFilter()
const { $t } = inject('i18n') as I18N

const props = defineProps({
    index: {
        type: Number,
        required: true,
    },
})

const eventRefs = computed(() => {
    return stepsStore.steps.map(step => {
        return step.events.map(event => {
            return event.event
        })
    }).flat()
})

const filterGroup = computed<FilterGroup | null>(() => {
    return filterGroupsStore.filterGroups[props.index] ?? null;
});

const conditionsItems = computed<UiSelectItemInterface<FilterCondition>[]>(() => {
    const conditions = (filterGroup.value?.filters.length ?? 0) > 1
        ? filterConditions
        : filterConditions.filter(item => item === 'and')

    return conditions.map(item => ({
        __type: 'item',
        id: item,
        label: $t(`filters.conditions.${item}`),
        value: item,
    }));
})

const removeFilterGroup = (): void => {
    filterGroupsStore.removeFilterGroup(props.index);
}

const changeFilterGroupCondition = (condition: FilterCondition): void => {
    filterGroupsStore.changeFilterGroupCondition({
        index: props.index,
        condition,
    });
}

const addFilterToGroup = (): void => {
    filterGroupsStore.addFilterToGroup({
        index: props.index,
        filter: {
            opId: OperationId.Eq,
            values: [],
            valuesList: [],
        }
    });
}

const removeFilterForGroup = (filterIndex: number): void => {
    filterGroupsStore.removeFilterForGroup({
        index: props.index,
        filterIndex,
    });
}

const changeFilterPropertyForGroup = async (filterIndex: number, payload: PropertyRef): Promise<void> => {
    filterGroupsStore.editFilterForGroup({
        index: props.index,
        filterIndex,
        filter: {
            propRef: payload,
            valuesList: await filterHelpers.getValues(payload),
        }
    });
}

const changeFilterOperationForGroup = (filterIndex: number, opId: OperationId): void => {
    filterGroupsStore.editFilterForGroup({
        index: props.index,
        filterIndex,
        filter: {
            opId,
        }
    });
}

const addFilterValueForGroup = (filterIdx: number, value: Value) => {
    filterGroupsStore.editFilterForGroup({
        index: props.index,
        filterIndex: filterIdx,
        filter: {
            values: [
                ...filterGroup.value?.filters[filterIdx].values ?? [],
                value,
            ]
        },
    });
}

const removeFilterValueForGroup = (filterIdx: number, value: Value) => {
    filterGroupsStore.editFilterForGroup({
        index: props.index,
        filterIndex: filterIdx,
        filter: {
            values: filterGroup.value
                ?.filters[filterIdx]
                .values
                .filter(item => item !== value)
            ?? [],
        },
    });
}
</script>
