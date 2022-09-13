<template>
    <ul class="pf-c-alert-group">
        <li
            v-for="item in props.items"
            :key="item.id"
            class="pf-c-alert-group__item"
        >
            <UiAlert
                :item="item"
                @close="closeItem(item.id)"
            />
        </li>
    </ul>
</template>

<script lang="ts" setup>
import UiAlert from './UiAlert.vue'
import { Alert } from '@/stores/alerts'

const props = defineProps<{
    items: Alert[]
}>()

const emit = defineEmits<{
    (e: 'close', id: string): void
}>()

const closeItem = (id: string) => emit('close', id)
</script>