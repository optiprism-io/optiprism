<template>
    <div class="ui-table">
        <div class="pf-c-toolbar">
            <div class="pf-c-toolbar__content">
                <div class="pf-c-toolbar__content-section pf-m-nowrap">
                    <div class="pf-c-toolbar__item">
                        <slot name="before" />
                    </div>
                    <div
                        v-if="props.showSelectColumns"
                        class="pf-c-toolbar__item pf-u-ml-auto"
                    >
                        <UiSelect
                            :items="columnsSelect"
                            :variant="'multiple'"
                            :text-button="columnsButtonText"
                            :selections="activeColumns"
                            @on-select="toggleColumns"
                        />
                    </div>
                </div>
            </div>
        </div>

        <table
            class="pf-c-table"
            :class="{
                'pf-m-compact': props.compact
            }"
            role="grid"
        >
            <thead>
                <tr
                    v-if="groups.length > 0"
                    role="row"
                >
                    <template
                        v-for="group in groups"
                        :key="group.value"
                    >
                        <UiTableCellWrapper
                            :fixed="group.fixed"
                            :last-fixed="group.lastFixed"
                            :colspan="group.span"
                        >
                            <UiTableHeadCell
                                :value="group.value"
                                :title="group.title"
                            />
                        </UiTableCellWrapper>
                    </template>
                </tr>
                <tr role="row">
                    <template
                        v-for="column in visibleColumns"
                        :key="column.value"
                    >
                        <UiTableCellWrapper
                            :fixed="column.fixed"
                            :sorted="column.sorted"
                            :truncate="column.truncate"
                            :last-fixed="column.lastFixed"
                            :type="column.type"
                        >
                            <UiTableHeadCell
                                :value="column.value"
                                :title="column.title"
                                :sorted="column.sorted"
                            />
                        </UiTableCellWrapper>
                    </template>
                </tr>
            </thead>
            <tbody role="rowgroup">
                <tr
                    v-for="(row, i) in visibleItems"
                    :key="i"
                    role="row"
                >
                    <template
                        v-for="(cell, j) in row"
                        :key="j"
                    >
                        <UiTableCellWrapper
                            :fixed="cell.fixed"
                            :truncate="cell.truncate"
                            :last-fixed="cell.lastFixed"
                            :no-wrap="cell.nowrap"
                            :type="cell.type"
                        >
                            <component
                                :is="cell.component || UiTableCell"
                                v-bind="cell"
                                :style="null"
                                @on-action="onAction"
                            />
                        </UiTableCellWrapper>
                    </template>
                </tr>
            </tbody>
        </table>
    </div>
</template>

<script lang="ts" setup>
import {Row, Column, Action, ColumnGroup} from '@/components/uikit/UiTable/UiTable'
import { computed, inject, onMounted, ref } from 'vue'
import UiTableHeadCell from '@/components/uikit/UiTable/UiTableHeadCell.vue'
import UiTableCell from '@/components/uikit/UiTable/UiTableCell.vue'
import UiTableCellWrapper from '@/components/uikit/UiTable/UiTableCellWrapper.vue'
import UiSelect, { UiSelectItem } from '@/components/uikit/UiSelect.vue'

const i18n = inject<any>('i18n')

type Props = {
    showSelectColumns?: boolean,
    compact?: boolean
    items?: Row[]
    columns: Column[]
    groups?: ColumnGroup[]
}

const props = withDefaults(defineProps<Props>(), {
    items: () => [],
    groups: () => [],
    compact: true,
    showSelectColumns: false,
})

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

const activeColumns = ref<string[]>([])
const columnsSelect = ref<UiSelectItem<string>[]>([])
const defaultColumns = ref<string[]>([])

const columnsButtonText = computed(() => {
    return `${columnsSelect.value.length} ${i18n.$t('common.columns')}`
})

const visibleColumns = computed(() => {
    return props.columns.filter(item => !props.showSelectColumns || item.default || activeColumns.value.includes(item.value))
})

const visibleItems = computed(() => {
    return props.items.map(row => {
        return row.filter(cell => !props.showSelectColumns || defaultColumns.value.includes(cell.key) || activeColumns.value.includes(cell.key))
    })
})

const onAction = (payload: Action) => {
    emit('on-action', payload)
}

const toggleColumns = (payload: string) => {
    if (activeColumns.value.includes(payload)) {
        activeColumns.value = activeColumns.value.filter(item => item !== payload)
    } else {
        activeColumns.value.push(payload)
    }
}

onMounted(() => {
    props.columns.map(item => {
        if (!item.notActiveStart) {
            activeColumns.value.push(item.value)
        }

        if (!item.default) {
            columnsSelect.value.push({
                key: item.value,
                nameDisplay: item.title,
                value: item.value,
            })
        }
    })
})
</script>

<style lang="scss">
.pf-c-table {
    tr > * {
        --pf-c-table--cell--MinWidth: 140px;
    }
}
</style>
