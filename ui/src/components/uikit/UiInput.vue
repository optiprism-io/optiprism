<template>
    <input
        :id="props.name"
        ref="input"
        class="pf-c-form-control"
        :value="value"
        :placeholder="props.placeholder"
        :min="props.min"
        :required="props.required"
        :name="props.name"
        @blur="blur"
    >
</template>

<script setup lang="ts">
import { onMounted, ref, computed } from 'vue'
const emit = defineEmits(['update:modelValue', 'blur']);

interface Props {
    modelValue?: number | string
    type?: string
    placeholder?: string
    min?: number
    mountFocus?: boolean
    required?: boolean
    name?: string
    label?: string
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

const value = computed({
    get() {
        return props.modelValue
    },
    set(value) {
        emit('update:modelValue', value)
    }
})

const blur = (e: any) => emit('blur', e);
</script>

<style lang="scss"></style>
