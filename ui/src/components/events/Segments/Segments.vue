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
            @on-remove-condition="onRemoveCondition"
            @change-action-condition="changeActionCondition"
            @change-property-condition="changePropertyCondition"
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
import { computed, inject, provide } from 'vue'
import { useSegmentsStore } from '@/stores/eventSegmentation/segments'
import Segment from '@/components/events/Segments/Segment.vue'
import { conditions } from '@/configs/events/conditions'
import { PropertyRef } from '@/types/events'
const i18n = inject<any>('i18n')

const segmentsStore = useSegmentsStore()

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
provide('conditionItems', conditionItems.value)

const addSegment = () => segmentsStore.addSegment(`${i18n.$t(`events.segments.segment`)} ${segmentsStore.segments.length + 1}`)
const deleteSegment = (idx: number) => segmentsStore.deleteSegment(idx)
const onRenameSegment = (name: string, idx: number) => segmentsStore.renameSegment(name, idx)
const addCondition = (idx: number) => segmentsStore.addConditionSegment(idx)
const onRemoveCondition = (idx: number, idxSegment: number) => segmentsStore.removeCondition(idx, idxSegment)
const changeActionCondition = (idx: number, idxSegment: number, ref: { id: string, name: string }) => segmentsStore.changeActionCondition(idx, idxSegment, ref)
const changePropertyCondition = (idx: number, idxSegment: number, ref: PropertyRef) => segmentsStore.changePropertyCondition(idx, idxSegment, ref)
</script>