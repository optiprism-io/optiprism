<template>
    <div class="ui-cell-tool-menu">
        <UiSelect
            :show-search="false"
            :items="selectItems"
            @update:model-value="onSelectMenu"
        >
            <UiButton
                class="pf-c-dropdown__toggle pf-m-plain pf-u-p-md"
            >
                <UiIcon :icon="'fas fa-ellipsis-v'" />
            </UiButton>
        </UiSelect>
    </div>
</template>

<script lang="ts" setup>
import { computed } from 'vue'
import { UiSelectGeneric } from '@/components/uikit/UiSelect/UiSelectGeneric'
import { Action, ToolMenuItem } from '@/components/uikit/UiTable/UiTable'
import UiButton from '@/components/uikit/UiButton.vue';
import { UiSelectItemInterface } from '@/components/uikit/UiSelect/types'

const UiSelect = UiSelectGeneric<string | number>()

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

interface Props {
    value: string | number
    items: ToolMenuItem[]
    type?: string
}

const props = defineProps<Props>()

const selectItems = computed<UiSelectItemInterface<string | number>[]>(() => {
    return props.items.map(item => ({
        __type: 'item',
        id: item.value,
        label: item.label,
        value: item.value
    }))
})

const onSelectMenu = (payload: string) => {
    emit('on-action', {
        type: props.value,
        name: payload
    })
}
</script>

<style>
.ui-cell-tool-menu {
    max-width: 40px;
}
</style>