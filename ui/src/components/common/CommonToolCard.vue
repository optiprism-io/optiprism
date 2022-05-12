<template>
    <div class="pf-c-card pf-m-compact pf-u-h-100">
        <div class="pf-c-card__title">
            <p>{{ title }}</p>
        </div>
        <div class="pf-c-card__body">
            <slot />
        </div>
    </div>
</template>

<script setup lang="ts">
defineProps({
    title: {
        type: String,
        default: '',
    },
})
</script>
