<template>
    <div
        class="ui-table-event-cell"
    >
        <div class="pf-u-display-flex pf-u-flex-direction-row pf-u-align-items-center">
            <div class="pf-l-flex__item">
                <div class="pf-u-display-flex pf-u-flex-direction-row pf-u-align-items-center">
                    <UiButtom
                        class="pf-u-text-nowrap pf-m-link pf-m-inline"
                        @click="props.action && onAction(props.action)"
                    >
                        {{ title }}
                    </UiButtom>
                </div>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed, defineComponent } from 'vue'
import { Action } from '../UiTable'
import UiButtom from '../../UiButton.vue'

type Props = {
    value: string | number
    title: string | number
    pinned?: boolean
    truncate?: boolean
    lastPinned?: boolean
    left?: number
    actions?: Action[]
    action?: Action,
    component?: ReturnType<typeof defineComponent>
}

const props = withDefaults(defineProps<Props>(), {
    left: 0,
})

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

const style = computed(() => {
    if (props.pinned) {
        return {
            left: props.left ? `${props.left}px` : undefined,
        }
    } else {
        return {}
    }
})

const onAction = (payload: Action) => {
    emit('on-action', payload)
}
</script>

<style lang="scss">
.ui-table-event-cell {
    width: auto !important;
    min-width: 380px !important;

    &__action-list {
        opacity: 0;
        cursor: pointer;
    }
}

.pf-c-table {
    tr:hover {
        .ui-table-event-cell__action-list {
            opacity: 1;
        }
    }
}
</style>