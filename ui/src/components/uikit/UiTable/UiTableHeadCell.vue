<template>
    <th
        :key="value"
        class="ui-table-th"
        :class="{
            'pf-c-table__sort': sorted,
            'pf-c-table__sticky-column': pinned,
            'pf-m-truncate': truncate,
            'pf-m-border-right': lastPinned,
        }"
        role="columnheader"
        aria-sort="none"
        :data-label="title"
        scope="col"
        :style="style"
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
    </th>
</template>

<script lang="ts" setup>
import { computed } from 'vue'

type Props = {
    value: string | number
    title: string | number
    sorted?: boolean
    pinned?: boolean
    truncate?: boolean
    left?: number
    lastPinned?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    left: 0,
})

const style = computed(() => {
    if (props.pinned) {
        return {
            left: props.left ? `${props.left}px` : undefined,
        }
    } else {
        return {};
    }
})
</script>

<style lang="scss" scoped></style>