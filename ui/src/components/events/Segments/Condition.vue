<template>
    <div class="condition pf-l-flex">
        <div class="pf-c-action-list">
            <div class="pf-c-action-list__item">
                <Select
                    :items="conditionItems"
                    :width-auto="true"
                    :is-open-mount="updateOpen"
                    :update-open="!isSelectedAction ? updateOpen : false"
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
            <div class="pf-c-action-list__item">
                <PropertySelect
                    v-if="isShowSelectProp"
                    @select="changeProperty"
                >
                    <UiButton
                        class="pf-m-main"
                        :class="{
                            'pf-m-secondary': isSelectedProp,
                        }"
                        type="button"
                        :before-icon="!isSelectedProp ? 'fas fa-plus-circle' : ''"
                    >
                        {{ displayNameProp }}
                    </UiButton>
                </PropertySelect>
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
import { PropertyRef } from '@/types/events'
import { Condition as ConditionType } from '@/types/events'
import Select from '@/components/Select/Select.vue'
import UiButton from '@/components/uikit/UiButton.vue'
import PropertySelect from '@/components/events/PropertySelect.vue'
import { conditions } from '@/configs/events/conditions'
import { useLexiconStore } from '@/stores/lexicon'
const i18n = inject<any>('i18n')

interface Props {
    index: number
    condition: ConditionType
    updateOpen?: boolean
}

const lexiconStore = useLexiconStore()
const props = defineProps<Props>()

const emit = defineEmits<{
    (e: 'on-remove', idx: number): void
    (e: 'change-action', idx: number, ref: { id: string, name: string }): void
    (e: 'change-property', idx: number, propRef: PropertyRef): void
}>()

const conditionItems = inject<[]>('conditionItems')

const isSelectedAction = computed(() => Boolean(props.condition.action))
const displayNameAction = computed(() => props.condition?.action?.name || i18n.$t(`events.segments.select_condition`))

const isSelectedProp = computed(() =>  Boolean(props.condition.propRef))
const displayNameProp = computed(() => props.condition.propRef ? lexiconStore.propertyName(props.condition.propRef) : i18n.$t(`events.select_property`))
const isShowSelectProp = computed(() => {
    const id = props.condition?.action?.id

    if (id && conditionItems) {
        const conditionItem = conditions.find(condition => condition.key === id)

        return Boolean(conditionItem?.hasProp)
    } else {
        return false
    }
})

const changeConditionAction = (payload: { id: string, name: string }) => emit('change-action', props.index, payload)
const onRemove = () => emit('on-remove', props.index)
const changeProperty = (propRef: PropertyRef) => emit('change-property', props.index, propRef)
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