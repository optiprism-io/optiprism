<template>
    <input
        ref="input"
        class="pf-c-form-control"
        :value="props.modelValue"
        :type="props.type"
        :placeholder="props.placeholder"
        :min="props.min"
        @input="updateValue"
        @blur="blur"
    >
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
const emit = defineEmits(["update:modelValue", "blur"]);

interface Props {
    modelValue?: number | string
    type?: string
    placeholder?: string
    min?: number
    mountFocus?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    modelValue: "",
    type: "text",
    placeholder: '',
    min: 0,
    mountFocus: false
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
    const target = e.target as HTMLInputElement;

    emit("update:modelValue", target.value);
};

const blur = (e: any) => emit("blur", e);
</script>

<style lang="scss"></style>
