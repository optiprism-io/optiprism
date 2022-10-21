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
                    @input="onInput($event, item.key)"
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
    key: string
    value: string | string[] | boolean
    editable?: boolean
    component?: ReturnType<typeof defineComponent> | 'p'
}

export type ActionPayload = {
    key: string
    value: string | string[] | boolean
}

type Props = {
    compact?: boolean
    horizontal?: boolean | undefined
    items: Item[]
}


const emit = defineEmits<{
    (e: 'onInput', payload: ActionPayload): void
}>()

const props = withDefaults(defineProps<Props>(), {
    compact: true
})

const onInput = (payload: Event | string | boolean, key: string) => {
    emit('onInput', {
        key,
        value: typeof payload === 'object' && !Array.isArray(payload) ? String(payload) : payload,
    })
}
</script>

<style lang="scss">
.ui-description-list {
    .pf-c-description-list {
        &__group {
            align-items: start;
        }
    }
}
</style>