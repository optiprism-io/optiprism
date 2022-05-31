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
export type Item = {
    label: string
    text: string
    type: 'label' | 'text'
}

type Props = {
    compact?: boolean
    horizontal?: boolean | undefined
    items: Item[]
}

const props = withDefaults(defineProps<Props>(), {
    compact: true
})
</script>