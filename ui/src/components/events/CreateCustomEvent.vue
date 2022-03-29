<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
        :title="$t('events.create_custom')"
        :description="$t('events.create_custom_description')"
        @apply="apply"
        @cancel="cancel"
    >
        <UiFormLabel
            :text="$t('events.custom_event_name')"
            :required="true"
            :for="'eventName'"
        >
            <UiInput
                v-model="eventName"
                :required="true"
                :name="'eventName'"
                :label="$t('events.custom_event_name')"
            />
        </UiFormLabel>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { ref } from 'vue'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiInput from '@/components/uikit/UiInput.vue'
import UiFormLabel from '../uikit/UiFormLabel.vue'

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()

const eventName = ref('')

const apply = () => {
    emit('apply')
}

const cancel = (type: string) => {
    emit('cancel')
}
</script>

<style lang="scss">
</style>