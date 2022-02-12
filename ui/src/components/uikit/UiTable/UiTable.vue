<template>
    <div class="ui-table">
        <table
            class="pf-c-table"
            role="grid"
            aria-label=""
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
                        aria-sort="none"
                        :data-label="column.title"
                        scope="col"
                        :style="`--pf-c-table--cell--MinWidth: ${column.minWidth || '120px'};`"
                    >
                        <button
                            v-if="column.sorted"
                            class="pf-c-table__button"
                        >
                            <div class="pf-c-table__button-content">
                                <span class="pf-c-table__text">{{ column.title }}</span>
                                <span class="pf-c-table__sort-indicator">
                                    <i class="fas fa-arrows-alt-v" />
                                </span>
                            </div>
                        </button>
                        <span
                            v-else
                            class="pf-c-table__text"
                        >{{ column.title }}</span>
                    </th>
                </tr>
            </thead>
            <tbody role="rowgroup">
                <tr
                    v-for="(row, i) in items"
                    :key="i"
                    role="row"
                >
                    <th
                        v-for="cell in row"
                        :key="cell.value"
                        :class="{
                            'pf-c-table__sticky-column': cell.pinned,
                            'pf-m-truncate': cell.truncate,
                            'pf-m-border-right': cell.lastPinned,
                        }"
                        role="columnheader"
                        :data-label="cell.title"
                        scope="col"
                    >
                        {{ cell.title }}
                    </th>
                </tr>
            </tbody>
        </table>
    </div>
</template>

<script lang="ts" setup>
import { Row, Column } from "@/components/uikit/UiTable/UiTable"

type Props = {
    items?: Row[];
    columns: Column[]
}

defineProps<Props>()
</script>

<style lang="scss" scoped>
.pf-c-table tr > * {
    --pf-c-table--cell--MaxWidth: auto;
    --pf-c-table--cell--Width: auto;
}
</style>