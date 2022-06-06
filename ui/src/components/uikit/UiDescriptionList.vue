<template>
    <dl
        class="pf-c-description-list"
        :class="{
            'pf-m-horizontal': props.horizontal,
            'pf-m-compact': props.compact,
        }"
    >
        <div
            v-for="item in props.items"
            :key="item.label"
            class="pf-c-description-list__group"
        >
            <dt class="pf-c-description-list__term">
                <span class="pf-c-description-list__text">{{ item.label }}</span>
            </dt>
            <dd class="pf-c-description-list__description">
                <div v-if="item.type === 'label'">
                    <span
                        v-for="label in item.text"
                        :key="label"
                        class="pf-c-label pf-m-blue pf-u-mr-sm"
                    >
                        <span class="pf-c-label__content">{{ label }}</span>
                    </span>
                </div>
                <div v-else-if="item.type === 'input'">
                    <UiInput
                        :value="item.text"
                        @input="onInput"
                    />
                </div>
                <div
                    v-else
                    class="pf-c-description-list__text"
                >
                    {{ item.text }}
                </div>
            </dd>
        </div>
    </dl>
</template>

<script lang="ts" setup>
import { UiSelectItem } from './UiSelect.vue'
import UiInput from './UiInput.vue'

export type Item = {
    label: string
    key?: string
    text: string
    type: 'label' | 'text' | 'input' | 'select'
    items?: UiSelectItem<string>[]
}

type Props = {
    compact?: boolean
    horizontal?: boolean | undefined
    items: Item[]
}


const emit = defineEmits<{
    (e: 'onInput', payload: string): void
}>()

const props = withDefaults(defineProps<Props>(), {
    compact: true
})

const onInput = (e: Event) => {
    const target = e.target as HTMLInputElement

    emit('onInput', target.value)
}
</script>