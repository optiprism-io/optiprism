<template>
    <span
        class="ui-tag pf-c-label pf-m-blue"
        @click="props.editable ? setEditing() : null"
    >
        <span class="pf-c-label__content">
            <input
                v-show="editing"
                ref="input"
                v-model="editingValue"
                class="ui-tag__input"
                :style="{
                    width: `${editingInputWidth}px`
                }"
                @blur="blur"
            >
            <span
                v-show="!editing"
                ref="text"
                class="ui-tag__text"
            >{{ props.value }}</span>
        </span>
    </span>
</template>

<script lang="ts" setup>
import { ref, onMounted } from 'vue'

const input = ref<HTMLCanvasElement | null>(null)
const text = ref<HTMLCanvasElement | null>(null)
const editing = ref(false)
const editingValue = ref('')
const editingInputWidth = ref(0)

const emit = defineEmits(['input']);

interface Props {
    editable?: boolean
    value: string
    index?: number
    mountFocus?: boolean
}

const props = defineProps<Props>()

onMounted(() => {
    if (props.mountFocus) {
        editingInputWidth.value = 60
        editing.value = true

        const inputElement = input.value

        setTimeout(() => {
            if (inputElement) {
                inputElement.focus()
            }
        })
    }
})

const setEditing = () => {
    const textElement = text.value
    if (textElement) {
        editingInputWidth.value = textElement.clientWidth;
    }

    editing.value = true
    editingValue.value = props.value

    const inputElement = input.value

    setTimeout(() => {
        if (inputElement) {
            inputElement.focus()
        }
    })
}

const blur = () => {
    emit('input', editingValue.value, props.index)
    editing.value = false
    editingValue.value = ''
    editingInputWidth.value = 0;
}
</script>

<style lang="scss">
.ui-tag {
    min-height: 29px;
    min-width: 20px;

    &__text {
        outline: none;
    }

    &__input {
        padding: 0;
        background-color: transparent;
        border: none;
        outline: none;
    }
}
</style>