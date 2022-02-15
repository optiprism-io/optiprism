<template>
    <td
        :class="{
            'pf-c-table__sticky-column': pinned,
            'pf-m-truncate': truncate,
            'pf-m-border-right': lastPinned,
        }"
        role="columnheader"
        :data-label="title"
        scope="col"
        :style="style"
    >
        {{ title }}
    </td>
</template>

<script lang="ts" setup>
import { computed } from 'vue'

type Props = {
    title: string | number
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