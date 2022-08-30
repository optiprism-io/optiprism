<template>
    <label class="pf-c-switch">
        <input
            class="pf-c-switch__input"
            type="checkbox"
            :checked="props.value"
            @input="updateValue"
        >
        <span class="pf-c-switch__toggle" />
        <span
            v-if="props.label"
            class="pf-c-switch__label"
        >
            {{ props.label }}
        </span>
    </label>
</template>

<script lang="ts" setup>
interface Props {
    modelValue?: number | string | boolean
    value?: boolean
    label?: string
}

const emit = defineEmits(['update:modelValue', 'input']);

const props = withDefaults(defineProps<Props>(), {
    modelValue: false,
    label: '',
});

const updateValue = (e: Event) => {
    const target = e.target as HTMLInputElement

    emit('update:modelValue', target.checked)
    emit('input', target.checked)
}
</script>