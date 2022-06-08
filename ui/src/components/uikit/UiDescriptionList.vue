<template>
    <dl
        class="ui-description-list pf-c-description-list"
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
            <dt class="pf-c-description-list__term pf-u-pt-xs">
                <span class="pf-c-description-list__text">{{ item.label }}</span>
            </dt>
            <dd class="pf-c-description-list__description">
                <component
                    :is="item.component"
                    :value="item.value"
                >
                    {{ item.value }}
                </component>
            </dd>
        </div>
    </dl>
</template>

<script lang="ts" setup>
import { defineComponent } from 'vue'

export type Item = {
    label: string
    key?: string
    value: string | string[]
    component?: ReturnType<typeof defineComponent> | 'p'
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

<style lang="scss">
.ui-description-list {
    .ui-textarea {
        min-height: 120px;
    }
    .pf-c-description-list {
        &__group {
            align-items: start;
        }
    }
}
</style>