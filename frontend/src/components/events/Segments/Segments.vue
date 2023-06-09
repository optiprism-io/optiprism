<template>
    <div class="segments">
        <Segment
            v-for="(item, index) in segmentsStore.segments"
            :key="item.name"
            :index="index"
            :name="item.name"
            :conditions="item.conditions || []"
            :auto-hide-event="!commonStore.showCreateCustomEvent"
            :is-one-segment="props.isOne"
            :is-last="(segmentsStore.segments.length - 1) === index"
            :segments-length="segmentsStore.segments.length"
            @on-remove="deleteSegment"
            @on-rename="onRenameSegment"
            @add-condition="addCondition"
        />
        <div
            v-if="!props.hideAddSegmentButton"
            class="pf-l-flex"
        >
            <UiButton
                class="pf-m-main"
                :is-link="true"
                :before-icon="'fas fa-plus'"
                @click="addSegment"
            >
                {{ $t('events.segments.add') }}
            </UiButton>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed, inject, provide } from 'vue'
import { OperationId, Value } from '@/types'
import {
    ChangeEventCondition,
    ChangeFilterPropertyCondition,
    RemoveFilterCondition,
    ChangeFilterOperation,
    FilterValueCondition,
    Ids,
    PeriodConditionPayload,
    PayloadChangeAgregateCondition,
    PayloadChangeValueItem,
    PayloadChangeEach,
} from '@/components/events/Segments/Segments'
import { useSegmentsStore } from '@/stores/reports/segments'
import { useEventsStore } from '@/stores/eventSegmentation/events'
import { useLexiconStore } from '@/stores/lexicon'
import { useCommonStore } from '@/stores/common'

import Segment from '@/components/events/Segments/Segment.vue'
import { aggregates } from '@/configs/events/segmentConditionDidEventAggregate'
import { PropertyRef } from '@/types/events'
import { DidEventCountTypeEnum } from '@/api'

const i18n = inject<any>('i18n')
const segmentsStore = useSegmentsStore()
const eventsStore = useEventsStore()
const lexiconStore = useLexiconStore()
const commonStore = useCommonStore()

const emit = defineEmits<{
    (e: 'get-event-segmentation'): void
}>()

type Props = {
    isOne?: boolean,
    hideAddSegmentButton?: boolean,
}

const props = defineProps<Props>();

const isFirstSegmentSelectAction = computed(() => {
    const lastSegment = segmentsStore.segments[0];
    const conditions = lastSegment?.conditions || [];
    return conditions[0]?.action;
});

const conditionAggregateItems = computed(() => {
    return aggregates.map(item => {
        const name = i18n.$t(`events.aggregates.${item.key}`)

        return {
            item: {
                id: item.key,
                name,
            },
            name,
            items: item.hasProperty ? lexiconStore.eventsQueryAggregates : null
        }
    })
})

const addSegment = () => {
    segmentsStore.addSegment(`${i18n.$t('events.segments.segment')} ${segmentsStore.segments.length + 1}`)
}
const deleteSegment = (idx: number) => segmentsStore.deleteSegment(idx)
const onRenameSegment = (name: string, idx: number) => segmentsStore.renameSegment(name, idx)
const addCondition = (idx: number) => segmentsStore.addConditionSegment(idx)
const changeActionCondition = (idx: number, idxSegment: number, ref: { id: string, name: string }) => {
    const segment = segmentsStore.segments[idxSegment];
    const conditions = segment?.conditions;
    const isNonSelectAction = !(conditions && conditions[idx].action?.id);
    segmentsStore.changeActionCondition(idx, idxSegment, ref);
    if (props.isOne && isNonSelectAction) {
        addCondition(0)
    }
}
const changePropertyCondition = (idx: number, idxSegment: number, ref: PropertyRef) => segmentsStore.changePropertyCondition(idx, idxSegment, ref)
const changeOperationCondition = (idx: number, idxSegment: number, opId: OperationId) => segmentsStore.changeOperationCondition(idx, idxSegment, opId)
const addValueCondition = (idx: number, idxSegment: number, value: Value) => segmentsStore.addValueCondition(idx, idxSegment, value)
const removeValueCondition = (idx: number, idxSegment: number, value: Value) => segmentsStore.removeValueCondition(idx, idxSegment, value)

provide('conditionAggregateItems', conditionAggregateItems.value)
provide('changeOperationCondition', changeOperationCondition)
provide('changePropertyCondition', changePropertyCondition)
provide('changeActionCondition', changeActionCondition)
provide('addValueCondition', addValueCondition)
provide('removeValueCondition', removeValueCondition)
provide('betweenAddCondition', (idx: number, indexParent: number, ref: {id: string, name: string}) => segmentsStore.betweenAddCondition(idx, indexParent, ref));
provide('changeAgregateCondition', (payload: PayloadChangeAgregateCondition) => segmentsStore.changeAgregateCondition(payload))
provide('onRemoveCondition', (payload: Ids) => segmentsStore.removeCondition(payload))
provide('addFilterCondition', (payload: Ids) => segmentsStore.addFilterCondition(payload))
provide('removeFilterCondition', (payload: RemoveFilterCondition) => segmentsStore.removeFilterCondition(payload))
provide('changeFilterPropertyCondition', (payload: ChangeFilterPropertyCondition) => segmentsStore.changeFilterPropertyCondition(payload))
provide('changeFilterOperation', (payload: ChangeFilterOperation) => segmentsStore.changeFilterOperation(payload))
provide('addFilterValueCondition', (payload: FilterValueCondition) => segmentsStore.addFilterValueCondition(payload))
provide('removeFilterValueCondition', (payload: FilterValueCondition) => segmentsStore.removeFilterValueCondition(payload))
provide('inputValueCondition', (payload: PayloadChangeValueItem) => segmentsStore.inputValue(payload))


/**
 * Period Calendar
 */
provide('changeEachCondition', (payload: PayloadChangeEach) => {
    segmentsStore.inputCalendarEach(payload)
})

provide('changePeriodCondition', (payload: PeriodConditionPayload) => {
    segmentsStore.changePeriodCondition(payload)
})


/**
 * Events condition
 */
provide('changeEventCondition', (payload: ChangeEventCondition) => {
    segmentsStore.changeEventCondition(payload)
    segmentsStore.changeAgregateCondition({
        idx: payload.idx,
        idxParent: payload.idxParent,
        value: {
            id: DidEventCountTypeEnum.Count,
            name: i18n.$t('events.aggregates.count')
        }
    })
})

provide('changeCompareEventCondition', (payload: ChangeEventCondition) => {
    segmentsStore.changeCompareEventCondition(payload)
})

provide('actionEvent', (payload: string) => {
    if (payload === 'createCustomEvent') {
        eventsStore.setEditCustomEvent(null)
        commonStore.togglePopupCreateCustomEvent(true)
    }
})

provide('editEvent', (payload: number) => {
    eventsStore.setEditCustomEvent(payload)
    commonStore.togglePopupCreateCustomEvent(true)
})

segmentsStore.$subscribe((mutation) => {
    if (mutation.type === 'direct') {
        emit('get-event-segmentation');
    }
});
</script>