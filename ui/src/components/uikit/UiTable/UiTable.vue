<template>
    <div class="ui-table">
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
                        v-for="column in columns"
                        :key="column.value"
                    >
                        <UiTableCellWrapper
                            :fixed="column.fixed"
                            :sorted="column.sorted"
                            :truncate="column.truncate"
                            :last-fixed="column.lastFixed"
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
                    v-for="(row, i) in items"
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
import UiTableHeadCell from '@/components/uikit/UiTable/UiTableHeadCell.vue'
import UiTableCell from '@/components/uikit/UiTable/UiTableCell.vue'
import UiTableCellWrapper from '@/components/uikit/UiTable/UiTableCellWrapper.vue'

type Props = {
    compact?: boolean
    items?: Row[]
    columns: Column[]
    groups?: ColumnGroup[]
}

const props = withDefaults(defineProps<Props>(), {
    items: () => [],
    groups: () => [],
    compact: true,
})

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

const onAction = (payload: Action) => {
    emit('on-action', payload)
}
</script>

<style lang="scss">
.pf-c-table {
    tr > * {
        --pf-c-table--cell--MinWidth: 140px;
        --pf-c-table--cell--MaxWidth: auto;
        --pf-c-table--cell--Width: auto;
    }
}
</style>
