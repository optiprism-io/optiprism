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
            <div
                v-if="isShowSelectProp"
                class="pf-c-action-list__item"
            >
                <PropertySelect
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
                v-if="props.condition.propRef && props.condition.opId"
                class="pf-c-action-list__item"
            >
                <OperationSelect
                    :property-ref="props.condition.propRef"
                    :selected="props.condition.opId"
                    @select="changeOperation"
                >
                    <UiButton class="pf-m-main pf-m-secondary">
                        {{ operationById?.get(props.condition.opId)?.name }}
                    </UiButton>
                </OperationSelect>
            </div>
            <div
                v-if="props.condition.propRef && props.condition.values"
                class="pf-c-action-list__item"
            >
                <ValueSelect
                    :property-ref="props.condition.propRef"
                    :selected="props.condition.values"
                    :items="conditionValuesItems"
                    @add="addValue"
                    @deselect="removeValue"
                >
                    <template v-if="props.condition.values.length > 0">
                        <div class="pf-c-action-list">
                            <div
                                v-for="(value, i) in props.condition.values"
                                :key="i"
                                class="pf-c-action-list__item"
                            >
                                <UiButton class="pf-m-main pf-m-secondary">
                                    {{ value }}
                                    <span class="pf-c-button__icon pf-m-end">
                                        <UiIcon
                                            icon="fas fa-times"
                                            @click.stop="removeValue(value)"
                                        />
                                    </span>
                                </UiButton>
                            </div>
                        </div>
                    </template>
                    <template v-else>
                        <UiButton
                            class="pf-m-main"
                            :before-icon="'fas fa-plus-circle'"
                        >
                            {{ $t('events.select_value') }}
                        </UiButton>
                    </template>
                </ValueSelect>
            </div>
            <div
                v-if="isShowSelectDate"
                class="pf-c-action-list__item"
            >
                <UiDatePicker
                    :value="calendarValue"
                    :last-count="lastCount"
                    :active-tab-controls="props.condition?.period?.type"
                    @on-apply="onApplyPeriod"
                >
                    <template #action>
                        <UiButton
                            class="pf-m-main"
                            :before-icon="'fas fa-calendar-alt'"
                            :class="{
                                'pf-m-secondary': isSelectedCalendar,
                            }"
                        >
                            {{ calendarValueString }}
                        </UiButton>
                    </template>
                </UiDatePicker>
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
import { operationById, OperationId, Value } from '@/types'
import { PropertyRef, Condition as ConditionType } from '@/types/events'
import Select from '@/components/Select/Select.vue'
import UiButton from '@/components/uikit/UiButton.vue'
import PropertySelect from '@/components/events/PropertySelect.vue'
import OperationSelect from '@/components/events/OperationSelect.vue'
import ValueSelect from '@/components/events/ValueSelect.vue'
import { conditions } from '@/configs/events/conditions'
import { useLexiconStore } from '@/stores/lexicon'
import { getStringDateByFormat } from '@/helpers/getStringDates'
import UiDatePicker, { ApplyPayload } from '@/components/uikit/UiDatePicker.vue'

const i18n = inject<any>('i18n')

interface Props {
    index: number
    indexParent: number
    condition: ConditionType
    updateOpen?: boolean
}

const lexiconStore = useLexiconStore()
const props = defineProps<Props>()

const emit = defineEmits<{
    (e: 'on-remove', idx: number): void
}>()

const conditionItems = inject<[]>('conditionItems')


const lastCount = computed(() => {
    return props.condition?.period?.last;
})

const calendarValue = computed(() => {
    return {
        from: props.condition?.period?.from || '',
        to: props.condition?.period?.to || '',
        multiple: false,
        dates: [],
    }
})

const isSelectedCalendar = computed(() => {
    return props.condition.period && props.condition.period.from && props.condition.period.to
})

const calendarValueString = computed(() => {
    if (props.condition.period && props.condition.period.from && props.condition.period.to) {
        switch(props.condition.period.type) {
            case 'last':
                return `${i18n.$t('common.calendar.last')} ${props.condition.period.last} ${props.condition.period.last === 1 ? i18n.$t('common.calendar.day') : i18n.$t('common.calendar.days')}`
            case 'since':
                return `${i18n.$t('common.calendar.since')} ${getStringDateByFormat(props.condition.period.from, '%d %b, %Y')}`
            case 'between':
                return `${getStringDateByFormat(props.condition.period.from, '%d %b, %Y')} - ${getStringDateByFormat(props.condition.period.to, '%d %b, %Y')}`
            default:
                return i18n.$t('common.select_period')
        }
    } else {
        return i18n.$t('common.select_period')
    }
})

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

const isShowSelectDate = computed(() => {
    const id = props.condition?.action?.id;

    return id === 'hadPropertyValue' && props.condition.action && props.condition.propRef && props.condition.values && props.condition.values.length
})

const conditionValuesItems = computed(() => {
    if (props.condition.valuesList) {
        return props.condition.valuesList.map((item: string, i) => {
            return { item, name: item }
        })
    } else {
        return []
    }
})

const onRemove = () => emit('on-remove', props.index)


const changePropertyCondition = inject<(idx: number, indexParent: number, propRef: PropertyRef) => void>('changePropertyCondition')
const changeOperationCondition = inject<(idx: number, indexParent: number, opId: OperationId) => void>('changeOperationCondition')
const changeActionCondition = inject<(idx: number, indexParent: number, ref: { id: string, name: string }) => void>('changeActionCondition')
const addValueCondition = inject<(idx: number, indexParent: number, value: Value) => void>('addValueCondition')
const removeValueCondition = inject<(idx: number, indexParent: number, value: Value) => void>('removeValueCondition')
const changePeriodCondition = inject<(idx: number, indexParent: number, payload: ApplyPayload) => void>('changePeriodCondition')
const changeConditionAction = (payload: { id: string, name: string }) =>
    changeActionCondition && changeActionCondition(props.index, props.indexParent, payload)

const changeProperty = (propRef: PropertyRef) =>
    changePropertyCondition && changePropertyCondition(props.index, props.indexParent, propRef)

const changeOperation = (opId: OperationId) =>
    changeOperationCondition && changeOperationCondition(props.index, props.indexParent, opId)

const addValue = (value: Value) =>
    addValueCondition && addValueCondition(props.index, props.indexParent, value)

const removeValue = (value: Value) =>
    removeValueCondition && removeValueCondition(props.index, props.indexParent, value)

const onApplyPeriod = (payload: ApplyPayload) =>
    changePeriodCondition && changePeriodCondition(props.index, props.indexParent, payload)
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