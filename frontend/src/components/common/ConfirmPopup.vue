<template>
    <UiPopupWindow
        :title="props.title"
        :apply-loading="props.loading"
        class="confirm-popup"
        :apply-button="props.applyButton || $t('common.ok')"
        :cancel-button="props.cancelButton || $t('common.cancel')"
        :apply-button-class="applyButtonClass"
        @apply="apply"
        @cancel="cancel"
    >
        <template v-if="$slots.content">
            <slot name="content" />
        </template>
        <div
            v-else
            class="confirm-popup__content"
            v-html="props.content"
        />
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { inject } from 'vue'
import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'

const i18n = inject<any>('i18n')

type Props = {
    title?: string
    content?: string
    loading?: boolean
    cancelButton?: string
    applyButton?: string
    applyButtonClass?: string
}

const props = defineProps<Props>()
const emit = defineEmits<{
    (e: 'apply'): void
    (e: 'cancel'): void
}>()

const apply = () => {
    emit('apply')
}

const cancel = () => {
    emit('cancel')
}
</script>