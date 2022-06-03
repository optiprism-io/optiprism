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
                <tr role="row">
                    <th
                        v-for="column in columns"
                        :key="column.value"
                        :class="{
                            'pf-c-table__sort': column.sorted,
                            'pf-c-table__sticky-column': column.pinned,
                            'pf-m-truncate': column.truncate,
                            'pf-m-border-right': column.lastPinned,
                        }"
                        role="columnheader"
                        scope="col"
                        :style="column.style"
                    >
                        <UiTableHeadCell
                            :value="column.value"
                            :title="column.title"
                            :sorted="column.sorted"
                        />
                    </th>
                </tr>
            </thead>
            <tbody role="rowgroup">
                <tr
                    v-for="(row, i) in items"
                    :key="i"
                    role="row"
                >
                    <td
                        v-for="cell in row"
                        :key="cell.value"
                        :class="{
                            'pf-c-table__sticky-column': cell.pinned,
                            'pf-m-truncate': cell.truncate,
                            'pf-m-border-right': cell.lastPinned,
                        }"
                        :style="cell.style"
                    >
                        <component
                            :is="cell.component || UiTableCell"
                            v-bind="cell"
                            :style="null"
                            @on-action="onAction"
                        />
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
</template>

<script lang="ts" setup>
import { computed } from 'vue'
import { Row, Column, Action } from '@/components/uikit/UiTable/UiTable'
import UiTableHeadCell from '@/components/uikit/UiTable/UiTableHeadCell.vue'
import UiTableCell from '@/components/uikit/UiTable/UiTableCell.vue'

type Props = {
    compact?: boolean
    items?: Row[]
    columns: Column[]
    stickyColumnMinWidth?: number
    stickyColumnWidth?: number
}

const props = withDefaults(defineProps<Props>(), {
    stickyColumnMinWidth: 170,
    stickyColumnWidth: 170,
    items: () => [],
    compact: true,
})

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

const stickyColumnMinWidth = computed(() => `${props.stickyColumnMinWidth}px`)
const stickyColumnWidth = computed(() => `${props.stickyColumnWidth}px`)

const onAction = (payload: Action) => {
    emit('on-action', payload)
}
</script>

<style lang="scss" scoped>
.pf-c-table {
    tr > * {
        --pf-c-table--cell--MinWidth: 140px;
        --pf-c-table--cell--MaxWidth: auto;
        --pf-c-table--cell--Width: auto;
    }

    &__sticky-column {
        --pf-c-table--cell--MinWidth: v-bind(stickyColumnWidth) !important;
        --pf-c-table--cell--MaxWidth: v-bind(stickyColumnWidth) !important;
        --pf-c-table__sticky-column--MinWidth: v-bind(stickyColumnMinWidth);
    }
}
</style>