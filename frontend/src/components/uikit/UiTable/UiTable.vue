<template>
    <div class="ui-table">
        <div
            v-if="props.showToolbar"
            class="pf-c-toolbar"
        >
            <div class="pf-c-toolbar__content">
                <div class="pf-c-toolbar__content-section pf-m-nowrap">
                    <div class="pf-c-toolbar__item">
                        <div class="pf-l-flex pf-u-align-items-center">
                            <slot name="before" />
                            <UiSpinner
                                v-show="props.isLoading"
                                class="pf-u-ml-md"
                                :size="'md'"
                            />
                        </div>
                    </div>
                    <div
                        v-if="slots.after"
                        class="pf-c-toolbar__item pf-u-ml-auto"
                    >
                        <slot name="after" />
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
                            :fit-content="column.fitContent"
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
import { computed, inject, useSlots, ref } from 'vue'
import UiTableHeadCell from '@/components/uikit/UiTable/UiTableHeadCell.vue'
import UiTableCell from '@/components/uikit/UiTable/UiTableCell.vue'
import UiTableCellWrapper from '@/components/uikit/UiTable/UiTableCellWrapper.vue'
import UiSelect, { UiSelectItem } from '@/components/uikit/UiSelect.vue'
import UiSpinner from '@/components/uikit/UiSpinner.vue'

const i18n = inject<any>('i18n')
const slots = useSlots();

type Props = {
    showSelectColumns?: boolean,
    compact?: boolean
    items?: Row[]
    columns: Column[]
    groups?: ColumnGroup[]
    isLoading?: boolean
    showToolbar?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    items: () => [],
    groups: () => [],
    compact: true,
    showSelectColumns: false,
    showToolbar: true,
})

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

const defaultColumns = ref<string[]>([])
const disabledColumns = ref<string[]>([])

const columnsSelect = computed(() => {
    return props.columns.reduce((acc: UiSelectItem<string>[], column) => {
        if (!column.default) {
            acc.push({
                key: column.value,
                nameDisplay: column.title,
                value: column.value,
            });
        }
        return acc;
    }, []);
});

const activeColumns = computed(() => {
    return props.columns.filter(item => !disabledColumns.value.includes(item.value)).map(item => item.value);
});

const columnsButtonText = computed(() => {
    return `${columnsSelect.value.length} ${i18n.$t('common.columns')}`
})

const visibleColumns = computed(() => {
    return props.columns.filter(item => !props.showSelectColumns || item.default || !disabledColumns.value.includes(item.value))
})

const visibleItems = computed(() => {
    return props.items.map(row => {
        return row.filter(cell => !props.showSelectColumns || defaultColumns.value.includes(cell.key) || !disabledColumns.value.includes(cell.key))
    })
})

const onAction = (payload: Action) => {
    emit('on-action', payload)
}

const toggleColumns = (payload: string) => {
    if (disabledColumns.value.includes(payload)) {
        disabledColumns.value = disabledColumns.value.filter(item => item !== payload)
    } else {
        disabledColumns.value.push(payload)
    }
}
</script>

<style lang="scss">
.ui-table {
    .pf-c-toolbar__content {
        min-height: 34px;
    }
}
</style>
