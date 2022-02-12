<template>
    <div
        class="pf-c-label-group"
        :class="{
            'pf-m-category': isCategory,
        }"
    >
        <div class="pf-c-label-group__main">
            <span
                id="label-group-category-label"
                class="pf-c-label-group__label"
                aria-hidden="true"
            >{{ props.label }}</span>
            <template v-if="$slots.content">
                <slot name="content" />
            </template>
        </div>
    </div>
</template>

<script lang="ts" setup>
export interface Props {
    label?: string
    isCategory?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    label: '',
    isCategory: false,
})

</script>

<style lang="scss" scoped>
</style>