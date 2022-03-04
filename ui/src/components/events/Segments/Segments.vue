<template>
    <div class="segments">
        <Segment
            v-for="(item, index) in segmentsStore.segments"
            :key="item.name"
            :index="index"
            :name="item.name"
            :conditions="item.conditions || []"
            @on-remove="deleteSegment"
            @on-rename="onRenameSegment"
            @add-condition="addCondition"
        />
        <div class="pf-l-flex">
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
import { computed, inject, provide, watch } from 'vue'
import { OperationId, Value } from '@/types'
import {
    ChangeEventCondition,
    ChangeFilterPropertyCondition,
    RemoveFilterCondition,
    ChangeFilterOperation,
    FilterValueCondition,
    Ids,
    PeriodConditionPayload,
} from '@/components/events/Segments/ConditionTypes'
import { useSegmentsStore } from '@/stores/eventSegmentation/segments'
import Segment from '@/components/events/Segments/Segment.vue'
import { conditions } from '@/configs/events/conditions'
import { PropertyRef } from '@/types/events'
import { useEventsStore } from '@/stores/eventSegmentation/events'
const i18n = inject<any>('i18n')

const segmentsStore = useSegmentsStore()
const eventsStore = useEventsStore()

const conditionItems = computed(() => {
    return conditions.map(item => {
        const name = i18n.$t(`events.condition.${item.key}`)

        return {
            item: {
                id: item.key,
                name,
            },
            name,
        }
    })
})

const addSegment = () => segmentsStore.addSegment(`${i18n.$t(`events.segments.segment`)} ${segmentsStore.segments.length + 1}`)
const deleteSegment = (idx: number) => segmentsStore.deleteSegment(idx)
const onRenameSegment = (name: string, idx: number) => segmentsStore.renameSegment(name, idx)
const addCondition = (idx: number) => segmentsStore.addConditionSegment(idx)
const changeActionCondition = (idx: number, idxSegment: number, ref: { id: string, name: string }) => segmentsStore.changeActionCondition(idx, idxSegment, ref)
const changePropertyCondition = (idx: number, idxSegment: number, ref: PropertyRef) => segmentsStore.changePropertyCondition(idx, idxSegment, ref)
const changeOperationCondition = (idx: number, idxSegment: number, opId: OperationId) => segmentsStore.changeOperationCondition(idx, idxSegment, opId)
const addValueCondition = (idx: number, idxSegment: number, value: Value) => segmentsStore.addValueCondition(idx, idxSegment, value)
const removeValueCondition = (idx: number, idxSegment: number, value: Value) => segmentsStore.removeValueCondition(idx, idxSegment, value)

provide('conditionItems', conditionItems.value)
provide('changeOperationCondition', changeOperationCondition)
provide('changePropertyCondition', changePropertyCondition)
provide('changeActionCondition', changeActionCondition)
provide('addValueCondition', addValueCondition)
provide('removeValueCondition', removeValueCondition)

provide('onRemoveCondition', (payload: Ids) => segmentsStore.removeCondition(payload))
provide('changePeriodCondition',  (payload: PeriodConditionPayload) => segmentsStore.changePeriodCondition(payload))
provide('addFilterCondition', (payload: Ids) => segmentsStore.addFilterCondition(payload))
provide('removeFilterCondition', (payload: RemoveFilterCondition) => segmentsStore.removeFilterCondition(payload))
provide('changeFilterPropertyCondition', (payload: ChangeFilterPropertyCondition) => segmentsStore.changeFilterPropertyCondition(payload))
provide('changeEventCondition', (payload: ChangeEventCondition) => segmentsStore.changeEventCondition(payload))
provide('changeFilterOperation', (payload: ChangeFilterOperation) => segmentsStore.changeFilterOperation(payload))
provide('addFilterValueCondition', (payload: FilterValueCondition) => segmentsStore.addFilterValueCondition(payload))
provide('removeFilterValueCondition', (payload: FilterValueCondition) => segmentsStore.removeFilterValueCondition(payload))

watch(
    segmentsStore.segments,
    () => {
        eventsStore.fetchEventSegmentationResult()
    }
)
</script>