<template>
    <UiPopupWindow
        :title="$t('reports.selectReport')"
        :cancel-button="$t('common.close')"
        @cancel="emit('cancel')"
    >
        <UiSpinner
            v-if="loading"
        />
        <UiTable
            v-else
            :show-toolbar="false"
            :items="itemsReport"
            :columns="[]"
            @on-action="onActionReport"
        />
    </UiPopupWindow>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import { Report } from '@/api'

import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import UiTablePressedCell from '@/components/uikit/UiTable/UiTablePressedCell.vue'
import UiSpinner from '@/components/uikit/UiSpinner.vue'
import { Action, Row } from '@/components/uikit/UiTable/UiTable'

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'on-select-report', id: number): void
}>()

const props = withDefaults(
    defineProps<{
        reports: Report[]
        loading: boolean
    }>(),
    {
        loading: false,
    }
);

const itemsReport = computed(() => {
    if (props.reports.length) {
        return props.reports.map((item): Row => {
            return [
                {
                    key: 'name',
                    title: item.name ?? '',
                    value: item.id,
                    component: UiTablePressedCell,
                    action: {
                        type: item.id,
                        name: item.name ?? '',
                    }
                },
                {
                    key: 'description',
                    title: item.description ?? '',
                    nowrap: true,
                },
            ]
        });
    } else {
        return []
    }
});

const onActionReport = (payload: Action) => {
    emit('on-select-report', Number(payload.type))
}
</script>

<style lang="scss">
</style>