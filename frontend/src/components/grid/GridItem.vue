<template>
    <div
        class="pf-l-grid__item"
        :class="[`pf-m-${col}-col`, `pf-m-${colLg}-col-on-lg`]"
    >
        <slot />
    </div>
</template>

<script setup lang="ts">
defineProps({
    col: {
        type: [String, Number],
        default: 12,
    },
    colLg: {
        type: [String, Number],
        default: 6,
    },
})
</script>
