<template>
    <div class="ui-table-event-cell">
        <div class="pf-u-display-flex pf-u-flex-direction-row pf-u-align-items-center">
            <div class="pf-l-flex__item">
                <div class="pf-u-display-flex pf-u-flex-direction-row pf-u-align-items-center">
                    <div
                        v-if="props.customEvents && props.customEvents.length"
                        class="pf-u-display-flex"
                    >
                        <div
                            v-for="event in props.customEvents"
                            :key="event.value"
                            class="pf-u-mr-md ui-table-event-cell__custom-event"
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
                            :after-icon="action.icon"
                            @click="onAction(action)"
                        >
                            {{ $t('events.live_stream.customEvent') }}
                        </UiButton>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { defineComponent, computed } from 'vue'
import { Action, Cell } from '@/components/uikit/UiTable/UiTable'

export type EventCell = Cell & {
    customEvents: {
        name: string,
        value: number
    }[]
}

type Props = {
    title: string | number
    actions?: Action[]
    customEvents?: {
        name: string,
        value: number
    }[],
    component?: ReturnType<typeof defineComponent>
}

const props = withDefaults(defineProps<Props>(), {
})

const emit = defineEmits<{
    (e: 'on-action', payload: Action): void
}>()

const hasAction = computed(() => props.actions && props.actions.length)

const customEventsMargin = computed(() => {
    return props.customEvents && props.customEvents.length >= 3
})

const onAction = (payload: Action) => {
    emit('on-action', payload)
}
</script>

<style lang="scss">
.ui-table-event-cell {
    max-width: 25rem;

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