<template>
    <td
        ref="cell"
        :class="{
            'pf-c-table__sticky-column': props.fixed,
            'pf-m-truncate': props.truncate,
            'pf-m-border-right': props.lastFixed,
            'pf-c-table__sort': props.sorted,
        }"
        :style="{
            left: left,
        }"
    >
        <slot />
    </td>
</template>

<script lang="ts" setup>
import { onMounted, ref } from 'vue'

type Props = {
   fixed?: boolean
   lastFixed?: boolean
   truncate?: boolean
   sorted?: boolean
}

const props = defineProps<Props>()
const cell = ref<HTMLElement | null>(null)
const left = ref('')

onMounted(() => {
    if (props.fixed) {
        const cellEl = cell.value;
        if (cellEl) {
            const parentEl = cellEl.parentElement;

            if (parentEl) {
                const boundingClientRectParent = parentEl.getBoundingClientRect()
                left.value = `${cellEl.offsetLeft - boundingClientRectParent.left}px`
            }
        }
    }
})
</script>

<style lang="scss"></style>