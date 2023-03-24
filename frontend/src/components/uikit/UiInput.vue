<template>
    <input
        :id="props.name"
        ref="input"
        class="pf-c-form-control"
        :value="props.modelValue || props.value"
        :placeholder="props.placeholder"
        :min="props.min"
        :required="props.required"
        :name="props.name"
        :type="props.type"
        :aria-invalid="invalid"
        :autocomplete="props.autocomplete"
        @input="updateValue"
        @blur="blur"
    >
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
const emit = defineEmits(['update:modelValue', 'blur', 'input']);

interface Props {
    modelValue?: number | string
    value?: number | string
    type?: string
    placeholder?: string
    min?: number
    mountFocus?: boolean
    required?: boolean
    name?: string
    label?: string
    error?: string
    invalid?: boolean
    autocomplete?: 'new-password' | 'current-password' | 'username'
}

const props = withDefaults(defineProps<Props>(), {
    modelValue: '',
    type: 'text',
    mountFocus: false,
    placeholder: undefined,
    min: undefined,
    name: undefined,
    label: undefined,
});

const input = ref<HTMLCanvasElement | null>(null)

onMounted(() => {
    if (props.mountFocus) {
        const inputElement = input.value
        if (inputElement) {
            inputElement.focus()
        }
    }
})

const updateValue = (e: Event) => {
    const target = e.target as HTMLInputElement

    emit('update:modelValue', target.value)
    emit('input', target.value)
}

const blur = (e: any) => emit('blur', e);
</script>

<style lang="scss"></style>
