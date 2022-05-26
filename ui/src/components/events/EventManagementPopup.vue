<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
        :title="title"
        :apply-loading="loading"
        @apply="apply"
        @cancel="cancel"
    />
</template>

<script lang="ts" setup>
import { computed, inject } from 'vue'
import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'

const i18n = inject<any>('i18n')

type Props = {
    name?: string
    loading?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    name: '',
})

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()


const title = computed(() => `${i18n.$t('events.event_management.event')}: ${props.name}`)

const apply = () => {
    emit('apply')
}

const cancel = () => {
    emit('cancel')
}
</script>
