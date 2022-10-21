<template>
    <i
        :class="[icon]"
        aria-hidden="true"
    />
</template>

<script setup lang="ts">
defineProps<{
    icon: string;
}>();
</script>
