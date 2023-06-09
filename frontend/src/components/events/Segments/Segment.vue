<template>
    <div
        class="segment pf-l-flex pf-m-column"
        :class="{
            'pf-u-mb-md': !props.isLast,
        }"
    >
        <div
            v-if="!props.isOneSegment"
            class="pf-l-flex"
        >
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
        <div
            class="segment__condition-list pf-l-flex pf-m-column"
            :class="{
                'pf-u-pl-xl': !props.isOneSegment,
            }"
        >
            <Condition
                v-for="(condition, i) in props.conditions"
                :key="i"
                :index="i"
                :condition="condition"
                :next-condition="props.conditions[i + 1] || null"
                :update-open="updateOpenCondition"
                :index-parent="props.index"
                :auto-hide-event="props.autoHideEvent"
                :is-one="props.isOneSegment"
                :allow-and-or="props.isOneSegment && i > 0 ? !['and', 'or'].includes(props.conditions[i - 1]?.action?.id || '') : false"
                :show-remove="props.isOneSegment ? conditionsLength > 1 && (conditionsLength !== i + 1) && (showRemoveCondition.length > 1 ? true : !!condition?.action) : true"
            />
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import AlphabetIdentifier from '@/components/common/identifier/AlphabetIdentifier.vue'
import UiEditableText from '@/components/uikit/UiEditableText.vue'
import Condition from '@/components/events/Segments/Condition.vue'
import { Condition as ConditionType } from '@/types/events'

interface Props {
    index: number
    name: string
    conditions: ConditionType[]
    autoHideEvent?: boolean
    isOneSegment?: boolean
    isLast?: boolean
    isActiveAndOrFilter?: boolean
    segmentsLength: number
}

const props = defineProps<Props>()

const emit = defineEmits<{
    (e: 'on-remove', inx: number): void
    (e: 'on-rename', name: string, idx: number): void
    (e: 'add-condition', idx: number): void
}>()

const updateOpenCondition = ref(false)

const showRemoveCondition = computed(() => {
    return props.conditions.filter(item => !item?.action?.id);
});

const conditionsLength = computed(() => {
    return props.conditions.length;
});

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
