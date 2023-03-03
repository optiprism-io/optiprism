<template>
    <div class="ui-input-tags">
        <UiInput
            v-if="edit || !props.value?.length"
            :value="inputValue"
            :mount-focus="Boolean(props.value?.length)"
            @input="onInput"
            @blur="onBlur"
        />
        <div
            v-else
            @click="setEdit"
        >
            <UiTags :value="props.value" />
        </div>
    </div>
</template>

<script lang="ts" setup>
import { ref } from 'vue'
import UiTags from './UiTags.vue'
import UiInput from './UiInput.vue'

const emit = defineEmits(['input'])

interface Props {
    value: string[]
}

const props = defineProps<Props>()
const inputValue = ref('')
const edit = ref(false)

const setEdit = () => {
    inputValue.value = props.value?.join(', ')
    edit.value = true
}

const onInput = (payload: string) => {
    inputValue.value = payload
}

const onBlur = () => {
    edit.value = false
    emit('input', inputValue.value.replace(/\s/g, '').split(',').filter(item => Boolean(item)))
}
</script>

<style lang="scss">
.ui-input-tags {
    cursor: pointer;
    min-height: 37px;

    .ui-tag {
        min-height: 34px;
    }
}
</style>