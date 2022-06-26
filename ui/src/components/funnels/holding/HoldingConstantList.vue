<template>
    <div
        v-if="holdingProperties.length > 0"
        class="pf-l-flex"
    >
        <span class="pf-l-flex__item">
            {{ $t('funnels.holdingConstant.holding') }}
        </span>

        <UiSelectProperty
            v-for="(props, index) in holdingProperties"
            :key="index"
            :items="propertiesItems"
            class="pf-l-flex__item"
            @update:model-value="editHoldingProperty(index, $event)"
        >
            <UiButton class="pf-m-main pf-m-secondary">
                {{ props.name }}

                <span class="pf-c-button__icon pf-m-end">
                    <UiIcon
                        icon="fas fa-times"
                        @click.stop="deleteHoldingProperty(index)"
                    />
                </span>
            </UiButton>

            <template #description="{ item }">
                {{ item.value?.label }}
            </template>
        </UiSelectProperty>
    </div>
</template>

<script lang="ts" setup>
import {HoldingProperty, useStepsStore} from '@/stores/funnels/steps';
import {computed} from 'vue';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';
import UiActionList from '@/components/uikit/UiActionList/UiActionList.vue';
import UiActionListItem from '@/components/uikit/UiActionList/UiActionListItem.vue';
import {useLexiconStore} from '@/stores/lexicon';

const UiSelectProperty = UiSelectGeneric<HoldingProperty>()

const lexiconStore = useLexiconStore();
const stepsStore = useStepsStore();

const holdingProperties = computed(() => stepsStore.holdingProperties)

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

const editHoldingProperty = (index: number, property: HoldingProperty) => {
    stepsStore.editHoldingProperty({index, property})
}

const deleteHoldingProperty = (index: number) : void => {
    stepsStore.deleteHoldingProperty(index)
}
</script>
