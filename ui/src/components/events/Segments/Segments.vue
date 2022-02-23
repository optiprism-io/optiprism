<template>
    <div class="segments">
        <Segment
            v-for="(item, index) in eventsStore.segments"
            :key="item.name"
            :index="index"
            :name="item.name"
            :conditions="item.conditions || []"
            @on-remove="deleteSegment"
            @on-rename="onRenameSegment"
            @add-condition="addCondition"
            @on-remove-condition="onRemoveCondition"
            @change-action-condition="changeActionCondition"
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
import {computed, inject, provide} from 'vue'
import {useLexiconStore} from '@/stores/lexicon'
import {useEventsStore} from '@/stores/eventSegmentation/events'
import Segment from '@/components/events/Segments/Segment.vue'
const i18n = inject<any>('i18n')

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();

const conditionItems = computed(() => {
    return lexiconStore.conditions.map(item => {
        const name = i18n.$t(`events.condition.${item}`)

        return {
            item: {
                id: item,
                name,
            },
            name,
        }
    })
})
provide('conditionItems', conditionItems.value)

const addSegment = () => eventsStore.addSegment()
const deleteSegment = (idx: number) => eventsStore.deleteSegment(idx)
const onRenameSegment = (name: string, idx: number) => eventsStore.renameSegment(name, idx)
const addCondition = (idx: number) => eventsStore.addConditionSegment(idx)
const onRemoveCondition = (idx: number, idxSegment: number) => eventsStore.removeCondition(idx, idxSegment)
const changeActionCondition = (idx: number, idxSegment: number, ref: { id: string, name: string }) => eventsStore.changeActionCondition(idx, idxSegment, ref)
</script>