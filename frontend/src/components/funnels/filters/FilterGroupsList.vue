<template>
    <div class="pf-l-flex pf-m-column">
        <FilterGroup
            v-for="(_, index) in filterGroups.filterGroups"
            :key="index"
            :index="index"
        />

        <div
            v-if="filterGroups.filterGroups.length === 0"
            class="pf-l-flex"
        >
            <UiButton
                class="pf-m-main"
                :is-link="true"
                :before-icon="'fas fa-plus'"
                @click="filterGroups.addFilterGroup"
            >
                Add group
            </UiButton>
        </div>
    </div>
</template>

<script lang="ts" setup>
import FilterGroup from '@/components/funnels/filters/FilterGroup.vue'
import { useFilterGroupsStore } from '@/stores/reports/filters'

const filterGroups = useFilterGroupsStore()
</script>
