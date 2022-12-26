<template>
    <PropertySelect
        :force-props="lexiconStore.eventProperties"
        @select="addHoldingConstant"
    >
        <UiButton
            class="pf-m-main"
            :is-link="true"
            :before-icon="'fas fa-plus'"
        >
            {{ $t('funnels.holdingConstant.add') }}
        </UiButton>
    </PropertySelect>
</template>

<script lang="ts" setup>
import {useLexiconStore} from '@/stores/lexicon';
import {useStepsStore} from '@/stores/funnels/steps';
import {useEventsStore} from '@/stores/eventSegmentation/events';
import PropertySelect from '@/components/events/PropertySelect.vue';
import {PropertyRef} from '@/types/events';
import { EventFilterByPropertyTypeEnum } from '@/api'

const lexiconStore = useLexiconStore();
const stepsStore = useStepsStore();
const eventsStore = useEventsStore();

const addHoldingConstant = (property: PropertyRef): void => {
    const { id, name } = property.type === 'user'
        ? lexiconStore.findUserPropertyById(Number(property.id))
        : property.type === 'custom'
            ? lexiconStore.findEventCustomPropertyById(Number(property.id))
            : lexiconStore.findEventPropertyById(Number(property.id));

    if (id && name) {
        stepsStore.addHoldingProperty({
            id,
            name,
            type: property.type as EventFilterByPropertyTypeEnum
        })
    }
}
</script>
