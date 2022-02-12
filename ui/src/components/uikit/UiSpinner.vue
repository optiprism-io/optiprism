<template>
    <span class="ui-spinner">
        <span
            class="pf-c-spinner"
            role="progressbar"
            aria-label="Loading..."
        >
            <span class="pf-c-spinner__clipper" />
            <span class="pf-c-spinner__lead-ball" />
            <span class="pf-c-spinner__tail-ball" />
        </span>
    </span>
</template>

<script setup lang="ts">
interface Props {
    size?: "sm" | "md" | "lg" | "xl" | string;
}

withDefaults(defineProps<Props>(), {
    size: "md"
});
</script>

<style lang="scss"></style>
