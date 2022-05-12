<template>
    <div class="pf-l-flex">
        <span class="pf-l-flex__item">within</span>

        <Select
            class="pf-l-flex__item"
            :items="sizeItems"
            @select="selectSize"
            @on-search="searchSize"
        >
            <UiButton
                class="pf-m-main pf-m-secondary"
                :is-link="true"
            >
                {{ stepsStore.size }}
            </UiButton>
        </Select>

        <Select
            class="pf-l-flex__item"
            :items="unitItems"
            :show-search="false"
            @select="selectUnit"
        >
            <UiButton
                class="pf-m-main pf-m-secondary"
                :is-link="true"
            >
                {{ $t(`common.timeUnits.${stepsStore.unit}`) }}
            </UiButton>
        </Select>

        <span class="pf-l-flex__item">
            {{ $t('criteria.timeWindow') }} {{ $t('criteria.in') }}
        </span>

        <Select
            class="pf-l-flex__item"
            :items="orderItems"
            :show-search="false"
            @select="selectOrder"
        >
            <UiButton
                class="pf-m-main pf-m-secondary"
                :is-link="true"
            >
                {{ $t(`criteria.orderType.${stepsStore.order}`) }}
            </UiButton>
        </Select>

        <span class="pf-l-flex__item">order</span>
    </div>
</template>

<script setup lang="ts">
import Select from '@/components/Select/Select.vue';
import {StepOrder, stepOrders, StepUnit, stepUnits, useStepsStore} from '@/stores/funnels/steps';
import {computed, inject, ref} from 'vue';
import {Item} from '@/components/Select/SelectTypes';
import {I18N} from '@/plugins/i18n';

const stepsStore = useStepsStore()
const i18n = inject<I18N>('i18n')

const dynamicSize = ref<Item<number> | null>(null)
const size = ref<number>(stepsStore.size)

const defaultSizes = Array.from({length: 10}).map((_, i) => {
    const value = 10 * (i + 1)
    return {
        item: value,
        name: `${value}`
    }
})

const sizeItems = computed<Item<number>[]>(() =>
    dynamicSize.value
        ? [...defaultSizes, dynamicSize.value]
        : defaultSizes
);

const unitItems = computed<Item<StepUnit>[]>(() => {
    return stepUnits.map(item => ({
        item,
        name: i18n?.$t(`common.timeUnits.${item}`) ?? item
    }))
})

const orderItems = computed<Item<StepOrder>[]>(() => {
    return stepOrders.map(item => ({
        item,
        name: i18n?.$t(`criteria.orderType.${item}`) ?? item
    }))
})

const searchSize = (value: string) => {
    const foundItem = sizeItems.value.find(item => {
        return item.name.toLowerCase().includes(value.toLowerCase())
    })

    if (!foundItem) {
        dynamicSize.value = {
            item: Number(value),
            name: value
        }
    } else {
        dynamicSize.value = null
    }
}

const selectSize = (value: number) => {
    stepsStore.setSize(value)
    dynamicSize.value = null
}

const selectUnit = (value: StepUnit) => {
    stepsStore.setUnit(value)
}

const selectOrder = (value: StepOrder) => {
    stepsStore.setOrder(value)
}
</script>
