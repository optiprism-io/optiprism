<template>
    <div
        :key="value"
        class="ui-table-th"
    >
        <button
            v-if="sorted"
            class="pf-c-table__button"
        >
            <div class="pf-c-table__button-content">
                <span class="pf-c-table__text">{{ title }}</span>
                <span class="pf-c-table__sort-indicator">
                    <i class="fas fa-arrows-alt-v" />
                </span>
            </div>
        </button>
        <span
            v-else
            class="pf-c-table__text"
        >{{ title }}</span>
    </div>
</template>

<script lang="ts" setup>

type Props = {
    value: string | number
    title: string | number
    sorted?: boolean
}

defineProps<Props>()
</script>

<style lang="scss" scoped></style>