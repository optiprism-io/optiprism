<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
        @apply="apply"
        @cancel="cancel"
    >
        Create Custom Event Content
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()

const apply = () => {
    emit('apply')
}

const cancel = (type: string) => {
    emit('cancel')
}
</script>

<style lang="scss">
</style>