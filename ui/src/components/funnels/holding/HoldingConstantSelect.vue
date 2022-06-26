<template>
    <UiSelectProperty
        :items="propertiesItems"
        @update:model-value="addHoldingConstant"
    >
        <UiButton
            class="pf-m-main"
            :is-link="true"
            :before-icon="'fas fa-plus'"
        >
            {{ $t('funnels.holdingConstant.add') }}
        </UiButton>

        <template #description="{ item }">
            {{ item.value?.label }}
        </template>
    </UiSelectProperty>
</template>

<script lang="ts" setup>
import {useLexiconStore} from '@/stores/lexicon';
import {HoldingProperty, useStepsStore} from '@/stores/funnels/steps';
import {computed} from 'vue';
import {useEventsStore} from '@/stores/eventSegmentation/events';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';

const UiSelectProperty = UiSelectGeneric<HoldingProperty>();

const lexiconStore = useLexiconStore();
const stepsStore = useStepsStore();
const eventsStore = useEventsStore();

const propertiesItems = computed<UiSelectItemInterface<HoldingProperty>[]>(() => {
    return [...lexiconStore.eventProperties, ...lexiconStore.eventCustomProperties]
        .filter(item => !stepsStore.holdingProperties.map(p => p.id).includes(item.id))
        .map(item => {
            return {
                __type: 'item',
                id: item.id,
                value: {
                    id: item.id,
                    name: item.name,
                },
                label: item.name,
            }
        })
})

const addHoldingConstant = (value: HoldingProperty): void => {
    stepsStore.addHoldingProperty(value);
}
</script>
