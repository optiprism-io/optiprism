<template>
    <div class="pf-c-action-list__item">
        <Select
            grouped
            :items="lexiconStore.eventsList"
            :width-auto="true"
            :auto-hide="autoHideEvent"
            @select="changeEvent"
            @action="selectAction"
            @edit="edit"
            @on-hover="onHoverEvent"
        >
            <UiButton
                class="pf-m-main"
                :class="{
                    'pf-m-secondary': props.condition.event,
                }"
                type="button"
                :before-icon="!props.condition.event ? 'fas fa-plus-circle' : ''"
            >
                {{ props.condition?.event?.name || $t('common.add_event') }}
            </UiButton>
            <template
                v-if="hoveredCustomEventId"
                #description
            >
                <SelectedEvent
                    v-for="(event, i) in hoveredCustomEventDescription"
                    :key="i"
                    :event="event"
                    :event-ref="event.ref"
                    :filters="event.filters"
                    :index="i"
                    :show-breakdowns="false"
                    :show-query="false"
                    :for-preview="true"
                />
            </template>
        </Select>
    </div>
    <div
        v-if="allowSelectAggregate"
        class="pf-c-action-list__item"
    >
        <Select
            :items="conditionAggregateItems"
            :width-auto="true"
            :is-open-mount="updateOpen"
            :update-open="!isSelectedAggregate ? updateOpen : false"
            @select="changeConditionAggregate"
        >
            <UiButton
                class="pf-m-main"
                :class="{
                    'pf-m-secondary': isSelectedAggregate,
                }"
                :before-icon="!isSelectedAggregate ? 'fas fa-plus-circle': ''"
            >
                {{ displayNameAggregate }}
            </UiButton>
        </Select>
    </div>
    <div
        v-if="isShowSelectProp"
        class="pf-c-action-list__item"
    >
        <PropertySelect @select="changeProperty">
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
        v-if="isShowSelectOpt && props.condition.opId"
        class="pf-c-action-list__item"
    >
        <OperationSelect
            :selected="props.condition.opId"
            :op-items="opItems"
            @select="changeOperation"
        >
            <UiButton class="pf-m-main pf-m-secondary">
                {{ operationButtonText }}
            </UiButton>
        </OperationSelect>
    </div>
    <div
        v-if="isShowNextEventSelect"
        class="pf-c-action-list__item"
    >
        <Select
            grouped
            :items="compareEventItems"
            :width-auto="true"
            @select="changeCompareEvent"
        >
            <UiButton
                class="pf-m-main"
                :class="{
                    'pf-m-secondary': props.condition.compareEvent,
                }"
                type="button"
                :before-icon="!props.condition.compareEvent ? 'fas fa-plus-circle' : ''"
            >
                {{ props.condition?.compareEvent?.name || $t('common.add_event') }}
            </UiButton>
        </Select>
    </div>
    <div
        v-if="isHasValue"
        class="pf-c-action-list__item"
    >
        <Select
            :items="valueList"
            :width-auto="true"
            :selected="props.condition.valueItem"
            @select="onInputValue"
        >
            <UiButton
                class="pf-m-main"
                :class="{
                    'pf-m-secondary': props.condition.event,
                }"
                type="button"
                :before-icon="!props.condition.valueItem ? 'fas fa-plus-circle' : ''"
            >
                {{ props.condition.valueItem }}
            </UiButton>
        </Select>
    </div>
</template>

<script lang="ts" setup>
import { inject, computed } from 'vue'

import { useLexiconStore } from '@/stores/lexicon'
import useCustomEvent from '@/components/events/Events/CustomEventHooks'

import { Item } from '@/components/Select/SelectTypes';
import { findOperations, operationById, OperationId } from '@/types'
import { PropertyRef, Condition as ConditionType, EventRef } from '@/types/events'
import { ChangeEventCondition, PayloadChangeAgregateCondition, PayloadChangeValueItem } from '@/components/events/Segments/Segments'

import { aggregates } from '@/configs/events/segmentConditionDidEventAggregate'
import { conditions } from '@/configs/events/segmentCondition'

import Select from '@/components/Select/Select.vue'
import PropertySelect from '@/components/events/PropertySelect.vue'
import OperationSelect from '@/components/events/OperationSelect.vue'
import SelectedEvent from '@/components/events/Events/SelectedEvent.vue'
import {
    DataType,
    DidEventRelativeCountTypeEnum,
} from '@/api'

const lexiconStore = useLexiconStore()
const { hoveredCustomEventDescription, hoveredCustomEventId, onHoverEvent } = useCustomEvent()
const i18n = inject<any>('i18n')

interface Props {
    index: number
    indexParent: number
    condition: ConditionType
    updateOpen?: boolean
    autoHideEvent?: boolean
}
const props = withDefaults(defineProps<Props>(), {
    autoHideEvent: true
})

const emit = defineEmits<{
    (e: 'change-property', propRef: PropertyRef): void
    (e: 'change-operation', opId: OperationId): void
}>()

const conditionItems = inject<[]>('conditionItems')
const conditionAggregateItems = inject<[]>('conditionAggregateItems')

const allIds = computed(() => ({idx: props.index, idxParent: props.indexParent}))

const conditionConfig = computed(() => {
    const id = props.condition?.action?.id

    if (id && conditionItems) {
        const conditionItem = conditions.find(condition => condition.key === id)

        return conditionItem || null
    } else {
        return null
    }
})


/**
 * Event
 */
const changeEventCondition = inject<(payload: ChangeEventCondition) => void>('changeEventCondition')
const changeCompareEventCondition = inject<(payload: ChangeEventCondition) => void>('changeCompareEventCondition')
const actionEvent = inject<(payload: string) => void>('actionEvent')
const editEvent = inject<(payload: number) => void>('editEvent')

const changeEvent = (ref: EventRef) => {
    changeEventCondition && changeEventCondition({
        idx: props.index,
        idxParent: props.indexParent,
        ref,
    })
}

const selectAction = (payload: string) => {
    actionEvent && actionEvent(payload)
}

const edit = (payload: number) => {
    editEvent && editEvent(payload)
}

const changeCompareEvent = (ref: EventRef) => {
    changeCompareEventCondition && changeCompareEventCondition({
        idx: props.index,
        idxParent: props.indexParent,
        ref,
    })
}

const isShowNextEventSelect = computed(() => {
    return props.condition.aggregate?.id === DidEventRelativeCountTypeEnum.RelativeCount
})

const compareEventItems = computed(() => {
    return lexiconStore.eventsList.map(eventGroup => {
        return {
            ...eventGroup,
            items: eventGroup.items.map(event => {
                return {
                    ...event,
                    disabled: props.condition.event?.ref.id === event.item.id,
                }
            })

        }
    })
})

/**
 * Agregate
 */
const didEventAggregateSelectedConfig = computed(() => {
    if (props.condition.aggregate) {
        const aggregate = aggregates.find(item => item.key === props.condition.aggregate?.id)

        return aggregate || null
    } else {
        return null
    }
})

const allowSelectAggregate = computed(() => conditionConfig.value && conditionConfig.value.hasSelectAggregate &&  Boolean(props.condition.event))

const isSelectedAggregate = computed(() => Boolean(props.condition.aggregate))

const displayNameAggregate = computed(() => {
    if (props.condition?.aggregate?.name) {
        return props.condition?.aggregate?.typeAggregate ? i18n.$t(`events.aggregateProperty.${props.condition.aggregate.typeAggregate}`) : props.condition?.aggregate?.name
    } else {
        return i18n.$t('common.select_aggregate')
    }
})

const changeAgregateCondition = inject<(payload: PayloadChangeAgregateCondition) => void>('changeAgregateCondition')

const changeConditionAggregate = (payload: { id: string, name: string }) => {
    changeAgregateCondition && changeAgregateCondition({
        ...allIds.value,
        value: payload,
    })
}


/**
 * Property
 */
const isShowSelectProp = computed(() => {
    const id = props.condition?.action?.id

    if (id && conditionItems) {

        return props.condition.aggregate && didEventAggregateSelectedConfig.value && didEventAggregateSelectedConfig.value.hasProperty
    } else {
        return false
    }
})
const displayNameProp = computed(() => props.condition.propRef ? lexiconStore.propertyName(props.condition.propRef) : i18n.$t('events.select_property'))
const isSelectedProp = computed(() =>  Boolean(props.condition.propRef))
const changeProperty = (propRef: PropertyRef) => emit('change-property', propRef)


/**
 * Operation
 */
const isShowSelectOpt = computed(() => {
    return isSelectedAggregate.value ? didEventAggregateSelectedConfig.value?.hasProperty ? Boolean(props.condition.propRef) : true : false
})

const operationButtonText = computed(() => {
    return props.condition.opId ? operationById?.get(props.condition.opId)?.shortName || operationById?.get(props.condition.opId)?.name : '';
})

const opItems = computed(() => {
    const items: Item<OperationId, null>[] = [];

    findOperations(DataType.Number, false, false).forEach(op =>
        items.push({
            item: op.id,
            name: op.name
        })
    )

    return items
})
const changeOperation = (opId: OperationId) => emit('change-operation', opId)


/**
 * Value
 */
const isHasValue = computed(() => {
    return !isShowNextEventSelect.value && isSelectedAggregate.value ? didEventAggregateSelectedConfig.value?.hasProperty ? Boolean(props.condition.propRef) : true : false
})

const valueList = computed(() => {
    const items = Array.from(Array(101).keys())
    items.shift()
    return items.map(item => ({
        item,
        name: String(item),
    }))
})

const inputValueCondition = inject<(payload: PayloadChangeValueItem) => void>('inputValueCondition')
const onInputValue = (payload: number) => {
    inputValueCondition && inputValueCondition({
        ...allIds.value,
        value: payload
    })
}
</script>