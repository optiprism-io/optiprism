<template>
    <div
        class="pf-c-inline-edit"
        :class="{
            'pf-m-inline-editable': isEditable,
        }"
    >
        <div class="pf-c-inline-edit__group">
            <div
                class="pf-c-inline-edit__value"
            >{{ value }}</div>
            <div class="pf-c-inline-edit__action pf-m-enable-editable">
            <button
                class="pf-c-button pf-m-plain"
                type="button"
                aria-label="Edit"
                @click="setEditable(true)"
            >
                <i class="fas fa-pencil-alt" aria-hidden="true"></i>
            </button>
            </div>
        </div>
        <div class="pf-c-inline-edit__group">
            <div class="pf-c-inline-edit__input">
            <input
                class="pf-c-form-control"
                type="text"
                value="Static value"
                @input="updateValue"
            />
            </div>
            <div class="pf-c-inline-edit__group pf-m-action-group pf-m-icon-group">
                <div class="pf-c-inline-edit__action pf-m-valid">
                    <button
                        class="pf-c-button pf-m-plain"
                        type="button"
                        aria-label="Save edits"
                        @click="onInput"
                    >
                        <i class="fas fa-check" aria-hidden="true"></i>
                    </button>
                </div>
                <div class="pf-c-inline-edit__action">
                    <button
                        class="pf-c-button pf-m-plain"
                        type="button"
                        aria-label="Cancel edits"
                        @click="setEditable(false)"
                    >
                        <i class="fas fa-times" aria-hidden="true"></i>
                    </button>
                </div>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { ref } from 'vue'

interface Props {
    value: number | string
}

const props = withDefaults(defineProps<Props>(), {
    value: '',
})

const emit = defineEmits([
    'on-input'
])

const valueEdit = ref<string | number>('')
const isEditable = ref(false)

const updateValue = (e: Event) => {
    const target = e.target as HTMLInputElement
    valueEdit.value = target.value
}

const setEditable = (payload: boolean) => {
    valueEdit.value = props.value
    isEditable.value = payload
}

const onInput = () => {
    emit('on-input', valueEdit.value)
    setEditable(false)
}
</script>