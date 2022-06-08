<template>
    <label class="pf-c-switch">
        <input
            class="pf-c-switch__input"
            type="checkbox"
            checked
        >
        <span class="pf-c-switch__toggle" />
        <span
            v-if="props.label"
            class="pf-c-switch__label pf-m-on"
        >
            {{ props.label }}
        </span>
    </label>
</template>

<script lang="ts" setup>
interface Props {
    modelValue: number | string | boolean
    label?: string
}


const props = withDefaults(defineProps<Props>(), {
    modelValue: false,
    label: '',
});
</script>