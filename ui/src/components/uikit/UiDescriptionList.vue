<template>
    <dl
        class="pf-c-description-list"
        :class="{
            'pf-m-horizontal': horizontal,
            'pf-m-compact': compact,
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
                <div class="pf-c-description-list__text">
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
    type: string
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