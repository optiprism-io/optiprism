<template>
    <UiCardTitle>
        {{ $t('funnels.filters') }}

        <template
            v-if="filterGroupsStore.filterGroups.length > 1"
            #extra
        >
            <div class="pf-l-flex">
                <span class="pf-l-flex__item">match</span>
                <UiSelectCondition
                    v-model="condition"
                    :items="conditionsItems"
                    :show-search="false"
                >
                    <UiButton
                        class="pf-m-main pf-m-secondary pf-l-flex__item"
                        :is-link="true"
                    >
                        {{ $t(`filters.conditions.${condition}`) }}
                    </UiButton>
                </UiSelectCondition>
                <span class="pf-l-flex__item">groups</span>
            </div>
        </template>
    </UiCardTitle>

    <UiCardBody>
        <FilterGroupsList />
    </UiCardBody>
</template>

<script setup lang="ts">
import { computed, inject } from 'vue'
import { I18N } from '@/utils/i18n'
import UiCardTitle from '@/components/uikit/UiCard/UiCardTitle.vue'
import UiCardBody from '@/components/uikit/UiCard/UiCardBody.vue'
import FilterGroupsList from '@/components/funnels/filters/FilterGroupsList.vue'
import { UiSelectGeneric } from '@/components/uikit/UiSelect/UiSelectGeneric'
import { FilterCondition, filterConditions, useFilterGroupsStore } from '@/stores/reports/filters'
import { UiSelectItemInterface } from '@/components/uikit/UiSelect/types'

const UiSelectCondition = UiSelectGeneric<FilterCondition>()
const filterGroupsStore = useFilterGroupsStore()
const { $t } = inject('i18n') as I18N

const condition = computed({
    get(): FilterCondition {
        return filterGroupsStore.condition
    },
    set(value: FilterCondition) {
        filterGroupsStore.setCondition(value)
    }
})

const conditionsItems = computed<UiSelectItemInterface<FilterCondition>[]>(() => {
    return filterConditions.map(item => ({
        __type: 'item',
        id: item,
        label: $t(`filters.conditions.${item}`),
        value: item,
    }))
})
</script>