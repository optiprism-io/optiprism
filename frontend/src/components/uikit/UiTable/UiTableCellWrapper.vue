<template>
    <td
        ref="cell"
        :class="{
            'pf-c-table__sticky-column': props.fixed,
            'pf-m-truncate': props.truncate,
            'pf-m-border-right': props.lastFixed,
            'pf-c-table__sort': props.sorted,
            'pf-c-table__action': props.type === 'action',
            'pf-u-text-nowrap': props.noWrap,
            'pf-m-fit-content': props.fitContent,
        }"
        :colspan="colspan"
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
   colspan?: number
   noWrap?: boolean
   type?: string
   fitContent?: boolean
}

const props = defineProps<Props>()
const cell = ref<HTMLElement | null>(null)
const left = ref('')

onMounted(() => {
    setTimeout(() => {
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
    }, 100)
})
</script>

<style lang="scss"></style>
