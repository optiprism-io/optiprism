<template>
    <textarea
        class="ui-textarea pf-c-form-control"
        ref="textarea"
        v-model="props.value"
        :placeholder="props.placeholder"
        @input="handleInput"
    >
    </textarea>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

const emit = defineEmits(['input', 'blur']);

interface Props {
    value: string | string[] | number | undefined
    placeholder?: string
    mountFocus?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    modelValue: '',
    mountFocus: false,
    placeholder: '',
});

const textarea = ref<HTMLCanvasElement | null>(null)

onMounted(() => {
    if (props.mountFocus) {
        const inputElement = textarea.value
        if (inputElement) {
            inputElement.focus()
        }
    }
})

const handleInput = (e: Event) => {
    const target = e.target as HTMLInputElement

    emit('input', target.value)
}

const blur = (e: any) => emit('blur', e);
</script>

<style lang="scss"></style>
