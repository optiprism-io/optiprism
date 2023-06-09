<template>
    <div
        class="condition pf-l-flex pf-m-column"
        :class="{
            'condition_is-one': props.isOne,
        }"
    >
        <div class="pf-c-action-list">
            <div class="pf-c-action-list__item">
                <Select
                    :items="allowAndOr ? conditionItemsAll : conditionItems"
                    :width-auto="true"
                    :is-open-mount="updateOpen || (!!nextCondition?.action && !!condition?.action)"
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
            <ConditionDidEvent
                v-if="isSelectedDidEvent"
                :index="props.index"
                :index-parent="props.indexParent"
                :condition="props.condition"
                :update-open="props.updateOpen"
                :auto-hide-event="props.autoHideEvent"
                @change-property="changeProperty"
                @change-operation="changeOperation"
            />
            <template v-else>
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
                            {{ operationButtonText }}
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
            </template>
            <div
                v-if="isShowSelectDate"
                class="pf-c-action-list__item"
            >
                <UiDatePicker
                    :value="calendarValue"
                    :last-count="lastCount"
                    :active-tab-controls="props.condition?.period?.type"
                    :show-each="true"
                    @on-change-each="onChangeEach"
                    @on-apply="onApplyPeriod"
                >
                    <template #action>
                        <UiButton
                            class="pf-m-main"
                            :before-icon="props.condition.each ? '' : 'fas fa-calendar-alt'"
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
                v-if="isHasFilter"
                class="pf-c-action-list__item condition__control"
                @click="addFilter"
            >
                <VTooltip popper-class="ui-hint">
                    <UiIcon icon="fas fa-filter" />
                    <template #popper>
                        {{ $t('common.add_filter') }}
                    </template>
                </VTooltip>
            </div>
            <div
                v-if="props.showRemove"
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
        <div
            v-if="filters.length"
            class="pf-l-flex pf-m-column pf-u-pl-xl"
        >
            <Filter
                v-for="(filter, i) in filters"
                :key="i"
                :event-ref="condition?.event?.ref"
                :filter="filter"
                :index="i"
                :update-open="updateOpenFilter"
                @remove-filter="removeFilter"
                @change-filter-property="changeFilterProperty"
                @change-filter-operation="onChangeFilterOperation"
                @add-filter-value="addFilterValue"
                @remove-filter-value="removeFilterValue"
            />
        </div>
        <div
            v-if="isAllowBetweenAdd"
            class="condition__between-add"
        >
            <Select
                :items="conditionItemsAll"
                :width-auto="true"
                :is-open-mount="updateOpenBetweenCondition"
                :update-open="updateOpenBetweenCondition"
                @select="changeBetweenAdd"
            >
                <UiButton
                    :before-icon="'fas fa-plus-circle'"
                    @click="betweenAdd"
                />
            </Select>
        </div>
        <i
            v-if="isAllowBetweenAdd"
            class="condition__between-add-after"
        />
    </div>
</template>

<script lang="ts" setup>
import { inject, computed, ref, defineAsyncComponent } from 'vue'
import { operationById, OperationId, Value } from '@/types'
import { PropertyRef, Condition as ConditionType } from '@/types/events'
import {
    DidEventRelativeCountTypeEnum,
} from '@/api';
import {
    ChangeFilterPropertyCondition,
    RemoveFilterCondition,
    ChangeFilterOperation,
    FilterValueCondition,
    Ids,
    PeriodConditionPayload,
    PayloadChangeEach,
} from '@/components/events/Segments/Segments'
import { conditions } from '@/configs/events/segmentCondition'
import { useLexiconStore } from '@/stores/lexicon'
import { getStringDateByFormat } from '@/helpers/getStringDates'
import { conditions as conditionsMap } from '@/configs/events/segmentCondition'
import usei18n from '@/hooks/useI18n';
import { Each, ApplyPayload } from '@/components/uikit/UiCalendar/UiCalendar'

import Select from '@/components/Select/Select.vue'
import UiButton from '@/components/uikit/UiButton.vue'
import PropertySelect from '@/components/events/PropertySelect.vue'
import OperationSelect from '@/components/events/OperationSelect.vue'
import ValueSelect from '@/components/events/ValueSelect.vue'
import Filter from '@/components/events/Filter.vue'
import UiDatePicker from '@/components/uikit/UiDatePicker.vue'
const ConditionDidEvent = defineAsyncComponent(() => import('./ConditionDidEvent.vue'))
const i18n = usei18n();

type Item = {
    id: string,
    name: string,
}

interface ItemConditionType {
    item: Item,
    items?: ItemConditionType[],
    name: string,
    description: string,
}

interface Props {
    index: number
    indexParent: number
    condition: ConditionType
    nextCondition: ConditionType | null,
    updateOpen?: boolean
    autoHideEvent?: boolean
    isOne?: boolean
    showRemove?: boolean
    allowAndOr?: boolean
}

const lexiconStore = useLexiconStore()
const props = withDefaults(defineProps<Props>(), {
    showRemove: true,
});

const getConditionItem = (key: string): ItemConditionType => {
    const name = i18n.t(`events.condition.${key}`) as string;
    const hintKey = `events.condition.${key}_hint`;

    return {
        item: {
            id: key,
            name,
        },
        name,
        description: i18n.keyExists(hintKey) ? i18n.t(hintKey) : '',
    }
}

const isAllowBetweenAdd = computed(() => {
    return props.isOne && props.condition?.action?.id && props.nextCondition?.action?.id;
});

const conditionItems = computed(() => {
    return conditionsMap.map(item => getConditionItem(item.key));
});

const conditionItemsAll = computed(() => {
    const items = [...conditionItems.value];
    items.push({
        item: {
            id: 'andOr',
            name: '',
        },
        items: ['and', 'or'].map(key => getConditionItem(key)),
        name: i18n.t('common.groupBy'),
        description: '',
    });
    return items;
});

const updateOpenFilter = ref(false);
const updateOpenBetweenCondition = ref(false);

const lastCount = computed(() => {
    return props.condition?.period?.last
})

const filters = computed(() => {
    return props.condition.filters
})

const calendarValue = computed(() => {
    return {
        from: props.condition?.period?.from || '',
        to: props.condition?.period?.to || '',
        multiple: false,
        dates: [],
        each: props.condition.each
    }
})

const isSelectedDidEvent = computed(() => {
    return props.condition?.action?.id === 'didEvent'
})

const isHasFilter = computed(() => {
    return props.condition?.action?.id === 'didEvent' && props.condition.event
})


/**
 * Calendar Period
 */
const isSelectedCalendar = computed(() => {
    return props.condition.each || props.condition.period && props.condition.period.from && props.condition.period.to
})

const calendarValueString = computed(() => {
    if (props.condition.period) {
        switch(props.condition.period.type) {
            case 'last':
                return `${i18n.t('common.calendar.last')} ${props.condition.period.last} ${props.condition.period.last === 1 ? i18n.t('common.calendar.day') : i18n.t('common.calendar.days')}`
            case 'since':
                return props.condition.period.from ? `${i18n.t('common.calendar.since')} ${getStringDateByFormat(props.condition.period.from, '%d %b, %Y')}` : ''
            case 'between':
                return props.condition.period.from && props.condition.period.to ? `${getStringDateByFormat(props.condition.period.from, '%d %b, %Y')} - ${getStringDateByFormat(props.condition.period.to, '%d %b, %Y')}` : ''
            case 'each':
                return `${i18n.t('common.calendar.each')} ${i18n.t(`common.calendar.each_select.${props.condition.each}`).toLowerCase()}`
            default:
                return i18n.t('common.select_period')
        }
    } else {
        return i18n.t('common.select_period')
    }
})

const conditionConfig = computed(() => {
    const id = props.condition?.action?.id

    if (id) {
        const conditionItem = conditions.find(condition => condition.key === id)

        return conditionItem || null
    } else {
        return null
    }
})

const isSelectedAction = computed(() => Boolean(props.condition.action))

const displayNameAction = computed(() => props.condition?.action?.name || (props.isOne ? i18n.t('events.segments.add_condition') : i18n.t('events.segments.select_condition')))

const isSelectedProp = computed(() =>  Boolean(props.condition.propRef))

const displayNameProp = computed(() => props.condition.propRef ? lexiconStore.propertyName(props.condition.propRef) : i18n.t('events.select_property'))

const isShowSelectProp = computed(() => {
    const id = props.condition?.action?.id

    if (id) {
        const conditionItem = conditions.find(condition => condition.key === id)

        return conditionItem?.hasProp;
    } else {
        return false
    }
})

const isShowSelectDate = computed(() => {
    if (isSelectedDidEvent.value) {
        return props.condition?.aggregate?.id === DidEventRelativeCountTypeEnum.RelativeCount || Boolean(props.condition.valueItem)
    } else {
        return conditionConfig.value && conditionConfig.value.hasSelectPeriod && props.condition.propRef && props.condition.values && props.condition.values.length
    }
})

const conditionValuesItems = computed(() => {
    if (props.condition.valuesList) {
        return props.condition.valuesList.map((item) => {
            return { item, name: item }
        })
    } else {
        return []
    }
})

const operationButtonText = computed(() => {
    return props.condition.opId ? operationById?.get(props.condition.opId)?.shortName || operationById?.get(props.condition.opId)?.name : '';
})

const changePropertyCondition = inject<(idx: number, indexParent: number, propRef: PropertyRef) => void>('changePropertyCondition')
const changeOperationCondition = inject<(idx: number, indexParent: number, opId: OperationId) => void>('changeOperationCondition')
const changeActionCondition = inject<(idx: number, indexParent: number, ref: { id: string, name: string }) => void>('changeActionCondition')
const addValueCondition = inject<(idx: number, indexParent: number, value: Value) => void>('addValueCondition')
const removeValueCondition = inject<(idx: number, indexParent: number, value: Value) => void>('removeValueCondition')

const addFilterCondition = inject<(payload: Ids) => void>('addFilterCondition')
const removeFilterCondition = inject<(payload: RemoveFilterCondition) => void>('removeFilterCondition')
const changeFilterPropertyCondition = inject<(payload: ChangeFilterPropertyCondition) => void>('changeFilterPropertyCondition')
const changeFilterOperation = inject<(payload: ChangeFilterOperation) => void>('changeFilterOperation')
const addFilterValueCondition = inject<(payload: FilterValueCondition) => void>('addFilterValueCondition')
const removeFilterValueCondition = inject<(payload: FilterValueCondition) => void>('removeFilterValueCondition')
const changePeriodCondition = inject<(payload: PeriodConditionPayload) => void>('changePeriodCondition')
const onRemoveCondition = inject<(payload: Ids) => void>('onRemoveCondition')
const changeEachCondition = inject<(payload: PayloadChangeEach) => void>('changeEachCondition')
const betweenAddCondition = inject<(idx: number, indexParent: number, ref: {id: string, name: string}) => void>('betweenAddCondition');

const changeConditionAction = (payload: { id: string, name: string }) => changeActionCondition && changeActionCondition(props.index, props.indexParent, payload)
const changeProperty = (propRef: PropertyRef) => changePropertyCondition && changePropertyCondition(props.index, props.indexParent, propRef)
const changeOperation = (opId: OperationId) => changeOperationCondition && changeOperationCondition(props.index, props.indexParent, opId)
const addValue = (value: Value) => addValueCondition && addValueCondition(props.index, props.indexParent, value)
const removeValue = (value: Value) => removeValueCondition && removeValueCondition(props.index, props.indexParent, value)

const onRemove = () => {
    onRemoveCondition && onRemoveCondition({
        idx: props.index,
        idxParent: props.indexParent,
    })
}

const onApplyPeriod = (payload: ApplyPayload) => {
    changePeriodCondition && changePeriodCondition({
        idx: props.index,
        idxParent: props.indexParent,
        value: payload
    })
}

const addFilter = () => {
    addFilterCondition && addFilterCondition({
        idx: props.index,
        idxParent: props.indexParent,
    })
    updateOpenFilter.value = true

    setTimeout(() => {
        updateOpenFilter.value = false
    })
}

const removeFilter = (idxFilter: number) => {
    removeFilterCondition && removeFilterCondition({
        idx: props.index,
        idxParent: props.indexParent,
        idxFilter
    })
}

const changeFilterProperty = (idxFilter: number, propRef: PropertyRef) => {
    changeFilterPropertyCondition && changeFilterPropertyCondition({
        idx: props.index,
        idxParent: props.indexParent,
        idxFilter,
        propRef
    })
}

const onChangeFilterOperation = (id: number, opId: OperationId) => {
    changeFilterOperation && changeFilterOperation({
        idx: props.index,
        idxParent: props.indexParent,
        idxFilter: id,
        opId,
    })
}

const addFilterValue = (id: number, value: Value) => {
    addFilterValueCondition && addFilterValueCondition({
        idx: props.index,
        idxParent: props.indexParent,
        idxFilter: id,
        value,
    })
}

const removeFilterValue = (id: number, value: Value) => {
    removeFilterValueCondition && removeFilterValueCondition({
        idx: props.index,
        idxParent: props.indexParent,
        idxFilter: id,
        value,
    })
}

const onChangeEach = (payload: Each) => {
    changeEachCondition && changeEachCondition({
        idx: props.index,
        idxParent: props.indexParent,
        value: payload,
    })
}

const betweenAdd = () => {
    updateOpenBetweenCondition.value = true
};

const changeBetweenAdd = (payload: {id: string, name: string}) => {
    betweenAddCondition && betweenAddCondition(props.index, props.indexParent, payload);
};
</script>

<style lang="scss">
.condition {
    position: relative;
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

    &_is-one {
        padding-left: 40px;
    }

    &__between-add-after {
        position: absolute;
        left: 40px;
        bottom: 0;
        height: 1px;
        width: calc(100% - 50px);
        background-color: #000;
        opacity: 0;
    }

    &__between-add {
        position: absolute;
        bottom: -28px;
        left: 0;
        opacity: 0;
        .pf-c-button {
            padding-top: 34px;
        }
        &:hover {
            opacity: 1;
            + .condition__between-add-after {
                opacity: 1;
            }
        }
    }
}
</style>