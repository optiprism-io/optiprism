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
    </UiSelectProperty>
</template>

<script lang="ts" setup>
import {useLexiconStore} from '@/stores/lexicon';
import {HoldingProperty, useStepsStore} from '@/stores/funnels/steps';
import {computed, watch} from 'vue';
import {EventProperty} from '@/types/events';
import {useEventsStore} from '@/stores/eventSegmentation/events';
import schemaService from '@/api/services/schema.service';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';

const UiSelectProperty = UiSelectGeneric<HoldingProperty>();

const lexiconStore = useLexiconStore();
const stepsStore = useStepsStore();
const eventsStore = useEventsStore();

const propertiesItems = computed<UiSelectItemInterface<HoldingProperty>[]>(() => {
    return stepsStore.propsAvailableToHold
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

const eventsIds = computed<number[]>(() => {
    return eventsStore.events.map(item => item.ref.id)
});

const getAvailableProperties = async (): Promise<void> => {
    const validProperties = lexiconStore.events
        .filter(item => eventsIds.value.includes(item.id))
        .map(item => item.properties ?? [])
        .flat()

    const res = await schemaService.eventProperties();
    const properties: HoldingProperty[] = res
        .map((item: EventProperty) => ({
            id: item.id,
            name: item.name,
        }))
        .filter((item: HoldingProperty) => validProperties.includes(item.id))

    stepsStore.setPropsAvailableToHold(properties);
}

watch(() => eventsIds.value.length, getAvailableProperties)

const addHoldingConstant = (value: HoldingProperty): void => {
    stepsStore.addHoldingProperty(value);
}
</script>
