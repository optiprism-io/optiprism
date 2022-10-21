<template>
    <div class="ui-editable-text">
        <VDropdown
            placement="bottom-end"
            :triggers="[]"
            :shown="isOpen"
            :disabled="props.disabledButton"
            popper-class="ui-editable-text-popup"
            @hide="onHide"
        >
            <div class="ui-editable-text__action">
                <UiInput
                    v-if="isOpen"
                    v-model="tempValue"
                    :type="'string'"
                    :mount-focus="true"
                    class="pf-u-p-0 pf-u-h-0"
                    @blur="onBlur"
                />
                <span
                    v-else
                    @click="onToggleInput"
                >
                    {{ props.value }}
                </span>
            </div>
            <template #popper="{ hide }">
                <UiButton
                    icon="fas fa-check"
                    class="pf-m-primary pf-m-small"
                    @click="onSave"
                />
                <UiButton
                    icon="fas fa-times"
                    class="pf-m-primary pf-m-small pf-u-ml-xs"
                    @click="($event: any) => {hide(); onHide()}"
                />
            </template>
        </VDropdown>
    </div>
</template>

<script lang="ts" setup>
import { ref, watch, onMounted } from 'vue'
import UiInput from '@/components/uikit/UiInput.vue'
import UiButton from '@/components/uikit/UiButton.vue'

interface Props {
    value: string
    disabledButton?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    value: '',
    disabledButton: true,
})

const emit = defineEmits<{
    (e: 'on-save', index: string): void
}>()

const tempValue = ref('')
const isOpen = ref(false)

onMounted(() => {
    tempValue.value = props.value
})

watch(() => props.value, (value) => {
    tempValue.value = value
})

const onHide = () => {
    isOpen.value = false
    tempValue.value = props.value
}

const onToggleInput = () => {
    isOpen.value = !isOpen.value
}

const onSave = () => {
    emit('on-save', tempValue.value)
    onHide()
}

const onBlur = () => {
    if (props.disabledButton) {
        onSave();
    }
}
</script>

<style lang="scss">
.ui-editable-text-popup {
    &.v-popper--theme-dropdown {
        .v-popper__inner {
            background-color: transparent;
            box-shadow: initial;
        }
    }
}

</style>

