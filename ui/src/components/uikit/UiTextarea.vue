<template>
    <textarea
        ref="textarea"
        :value="props.value"
        class="ui-textarea pf-c-form-control"
        :placeholder="props.placeholder"
        :rows="props.rows"
        @input="handleInput"
    />
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

const emit = defineEmits(['input', 'blur']);

interface Props {
    value: string | number | undefined
    placeholder?: string
    mountFocus?: boolean
    rows?: number
}

const props = withDefaults(defineProps<Props>(), {
    modelValue: '',
    mountFocus: false,
    placeholder: '',
    rows: 3
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