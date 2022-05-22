<template>
    <UiActionList v-if="holdingProperties.length > 0">
        <template #main>
            <div
                class="pf-l-flex"
            >
                <span class="pf-l-flex__item">
                    {{ $t('funnels.holdingConstant.holding') }}
                </span>

                <UiSelectProperty
                    v-for="(item, index) in holdingProperties"
                    :key="index"
                    :items="propertiesItems"
                    class="pf-l-flex__item"
                    @update:model-value="editHoldingProperty(index, $event)"
                >
                    <UiButton class="pf-m-main pf-m-secondary">
                        {{ item.name }}

                        <span class="pf-c-button__icon pf-m-end">
                            <UiIcon
                                icon="fas fa-times"
                                @click.stop="deleteHoldingProperty(index)"
                            />
                        </span>
                    </UiButton>
                </UiSelectProperty>
            </div>
        </template>

        <UiActionListItem @click="stepsStore.clearHoldingProperties">
            <VTooltip popper-class="ui-hint">
                <UiIcon icon="fas fa-trash" />
                <template #popper>
                    {{ $t('funnels.holdingConstant.clear') }}
                </template>
            </VTooltip>
        </UiActionListItem>
    </UiActionList>
</template>

<script lang="ts" setup>
import {HoldingProperty, useStepsStore} from '@/stores/funnels/steps';
import {computed} from 'vue';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';
import UiActionList from '@/components/uikit/UiActionList/UiActionList.vue';
import UiActionListItem from '@/components/uikit/UiActionList/UiActionListItem.vue';

const UiSelectProperty = UiSelectGeneric<HoldingProperty>()

const stepsStore = useStepsStore();

const holdingProperties = computed(() => stepsStore.holdingProperties)

const propertiesItems = computed<UiSelectItemInterface<HoldingProperty>[]>(() => {
    return stepsStore.propsAvailableToHold.map(item => {
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
