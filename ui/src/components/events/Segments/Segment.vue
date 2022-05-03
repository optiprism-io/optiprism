<template>
    <div class="segment pf-l-flex pf-m-column">
        <div class="pf-l-flex">
            <AlphabetIdentifier
                class="pf-l-flex__item"
                :index="props.index"
            />
            <div class="pf-c-action-list">
                <div class="pf-c-action-list__item">
                    <UiEditableText
                        :value="name"
                        @on-save="onRename"
                    >
                        <span>{{ name }}</span>
                    </UiEditableText>
                </div>
                <div
                    class="pf-c-action-list__item segment__control"
                    @click="addCondition"
                >
                    <VTooltip popper-class="ui-hint">
                        <UiIcon icon="fas fa-filter" />
                        <template #popper>
                            {{ $t('events.segments.add_condition') }}
                        </template>
                    </VTooltip>
                </div>
                <div
                    class="pf-c-action-list__item segment__control"
                    @click="onRemove"
                >
                    <VTooltip popper-class="ui-hint">
                        <UiIcon icon="fas fa-times" />
                        <template #popper>
                            {{ $t('events.segments.remove') }}
                        </template>
                    </VTooltip>
                </div>
            </div>
        </div>
        <div class="pf-l-flex pf-m-column pf-u-pl-xl">
            <Condition
                v-for="(condition, i) in props.conditions"
                :key="i"
                :index="i"
                :condition="condition"
                :update-open="updateOpenCondition"
                :index-parent="props.index"
                :auto-hide-event="props.autoHideEvent"
            />
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import AlphabetIdentifier from '@/components/common/AlphabetIdentifier.vue'
import UiEditableText from '@/components/uikit/UiEditableText.vue'
import Condition from '@/components/events/Segments/Condition.vue'
import { Condition as ConditionType } from '@/types/events'

interface Props {
    index: number
    name: string
    conditions: ConditionType[]
    autoHideEvent?: boolean
}

const props = defineProps<Props>()

const emit = defineEmits<{
    (e: 'on-remove', inx: number): void
    (e: 'on-rename', name: string, idx: number): void
    (e: 'add-condition', idx: number): void
}>()

const updateOpenCondition = ref(false)

const onRename = (name: string): void => emit('on-rename', name, props.index)
const addCondition = (): void => {
    updateOpenCondition.value = true
    emit('add-condition', props.index)

    setTimeout(() => {
        updateOpenCondition.value = false
    })
}
const onRemove = (): void => emit('on-remove', props.index)
</script>

<style scoped lang="scss">
.segment {
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
        .segment {
            &__control {
                opacity: 1;
            }
        }
    }
}
</style>
