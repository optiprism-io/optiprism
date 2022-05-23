<template>
    <td
        :class="{
            'pf-c-table__sticky-column': pinned,
            'pf-m-truncate': truncate,
            'pf-m-border-right': lastPinned,
        }"
        class="ui-table-event-cell"
        role="columnheader"
        :data-label="title"
        scope="col"
        :style="style"
    >
        <div class="pf-u-display-flex pf-u-flex-direction-row pf-u-align-items-center">
            <div class="pf-l-flex__item">
                <div class="pf-u-display-flex pf-u-flex-direction-row pf-u-align-items-center">
                    <div class="pf-u-text-nowrap">
                        {{ title }}
                    </div>

                    <div
                        v-if="props.customEvents && props.customEvents.length"
                        class="pf-u-display-flex pf-u-flex-wrap"
                    >
                        <div
                            v-for="event in props.customEvents"
                            :key="event.value"
                            class="pf-u-ml-md ui-table-event-cell__custom-event"
                            :class="{
                                'pf-u-mb-md': customEventsMargin,
                            }"
                            @click="onAction({
                                name: String(event.value),
                                icon: '',
                                type: 'event'
                            })"
                        >
                            <UiButton class="pf-m-main pf-m-secondary">
                                {{ event.name }}
                            </UiButton>
                        </div>
                    </div>
                </div>
            </div>
            <div
                v-if="hasAction"
                class="pf-l-flex__item pf-u-ml-auto"
            >
                <div class="pf-c-action-list ui-table-event-cell__action-list">
                    <div
                        v-for="action in props.actions"
                        :key="action.name"
                        class="pf-c-action-list__item"
                    >
                        <UiButton
                            class="pf-m-link"
                            :before-icon="action.icon"
                            @click="onAction(action)"
                        />
                    </div>
                </div>
            </div>
        </div>
    </td>
</template>

<script lang="ts" setup>
import { computed } from 'vue'
import { Action } from '../UiTable'

type Props = {
    title: string | number
    pinned?: boolean
    truncate?: boolean
    left?: number
    lastPinned?: boolean
    actions?: Action[]
    customEvents?: {
        name: string,
        value: number
    }[]
}

const props = withDefaults(defineProps<Props>(), {
    left: 0,
})

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

const hasAction = computed(() => props.actions && props.actions.length)

const style = computed(() => {
    if (props.pinned) {
        return {
            left: props.left ? `${props.left}px` : undefined,
        }
    } else {
        return {}
    }
})

const customEventsMargin = computed(() => {
    return props.customEvents && props.customEvents.length >= 3
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