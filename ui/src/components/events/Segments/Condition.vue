<template>
    <div class="condition pf-l-flex">
        <div class="pf-c-action-list">
            <div class="pf-c-action-list__item">
                <Select
                    :items="conditionItems"
                    :width-auto="true"
                    :is-open-mount="true"
                    @select="changeConditionAction"
                >
                    <UiButton
                        class="pf-m-main"
                        :class="{
                            'pf-m-secondary': isSelectedAction,
                        }"
                        :before-icon="!isSelectedAction ? 'fas fa-plus-circle': ''"
                    >
                        {{ displayNameAction }}
                    </UiButton>
                </Select>
            </div>
            <div
                class="pf-c-action-list__item condition__control"
                @click="onRemove"
            >
                <VTooltip popper-class="ui-hint">
                    <UiIcon icon="fas fa-times" />
                    <template #popper>
                        {{ $t('events.segments.remove_condition') }}
                    </template>
                </VTooltip>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { inject, computed } from 'vue'
import { Condition as ConditionType } from '@/types/events'
import Select from '@/components/Select/Select.vue'
import UiButton from '@/components/uikit/UiButton.vue'
const i18n = inject<any>('i18n')

interface Props {
    index: number
    condition: ConditionType
}

const props = defineProps<Props>()

const emit = defineEmits<{
    (e: 'on-remove', index: number): void
    (e: 'change-action', index: number, ref: { id: string, name: string }): void
}>()

const conditionItems = inject('conditionItems')

const isSelectedAction = computed(() => Boolean(props.condition.action))
const displayNameAction = computed(() => {
    return props.condition?.action?.name || i18n.$t(`events.segments.select_condition`)
})

const changeConditionAction = (payload: { id: string, name: string }) => emit('change-action', props.index, payload)
const onRemove = () => emit('on-remove', props.index)
</script>

<style lang="scss" scoped>
.condition {
    &__control {
        padding: 5px;
        opacity: 0;
        cursor: pointer;
        color: var(--op-base-color-text);

        &:hover {
            color: var(--pf-global--palette--black-800);
        }
    }

    &:hover {
        .condition {
            &__control {
                opacity: 1;
            }
        }
    }
}
</style>