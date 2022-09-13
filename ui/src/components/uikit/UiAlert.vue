<template>
    <div
        class="pf-c-alert pf-m-inline"
        :class="typeClass"
    >
        <div class="pf-c-alert__icon">
            <i
                class="fas fa-fw"
                :class="config[props.item.type]"
                aria-hidden="true"
            />
        </div>
        <p class="pf-c-alert__title">
            {{ props.item.text }}
        </p>
        <div
            v-if="!props.item.noClose"
            class="pf-c-alert__action"
            @click="closeItem(props.item.id)"
        >
            <button
                class="pf-c-button pf-m-plain"
                type="button"
            >
                <i
                    class="fas fa-times"
                    aria-hidden="true"
                />
            </button>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed } from 'vue'
import { Alert } from '@/stores/alerts'

const props = defineProps<{
    item: Alert
}>()

const emit = defineEmits<{
    (e: 'close', id: string): void
}>()

const config = {
    default: 'fa-bell',
    info: 'fa-info-circle',
    success: 'fa-check-circle',
    warning: 'fa-exclamation-triangle',
    danger: 'fa-exclamation-circle',
}

const typeClass = computed(() => `pf-m-${props.item.type}`)

const closeItem = (id: string) => emit('close', id)
</script>

<style lang="scss">
</style>