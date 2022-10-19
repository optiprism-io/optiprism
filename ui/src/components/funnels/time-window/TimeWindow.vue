<template>
    <div class="pf-l-flex">
        <span class="pf-l-flex__item">within</span>

        <UiSelectSize
            v-model="size"
            :items="sizeItems"
            @search="handleSizeSearch"
        >
            <UiButton
                class="pf-m-main pf-m-secondary pf-l-flex__item"
                :is-link="true"
            >
                {{ stepsStore.size }}
            </UiButton>
        </UiSelectSize>

        <UiSelectUnit
            v-model="unit"
            :items="unitItems"
            :show-search="false"
        >
            <UiButton
                class="pf-m-main pf-m-secondary pf-l-flex__item"
                :is-link="true"
            >
                {{ $t(`common.timeUnits.${stepsStore.unit}`) }}
            </UiButton>
        </UiSelectUnit>

        <span class="pf-l-flex__item">
            {{ $t('criteria.timeWindow') }} {{ $t('criteria.in') }}
        </span>

        <UiSelectOrder
            v-model="order"
            :items="orderItems"
            :show-search="false"
        >
            <UiButton
                class="pf-m-main pf-m-secondary pf-l-flex__item"
                :is-link="true"
            >
                {{ $t(`criteria.orderType.${stepsStore.order}`) }}
            </UiButton>
        </UiSelectOrder>

        <span class="pf-l-flex__item">order</span>
    </div>
</template>

<script setup lang="ts">
import {StepOrder, stepOrders, StepUnit, stepUnits, useStepsStore} from '@/stores/funnels/steps';
import {computed, inject, ref} from 'vue';
import {I18N} from '@/utils/i18n';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';

const UiSelectSize = UiSelectGeneric<number>();
const UiSelectUnit = UiSelectGeneric<StepUnit>();
const UiSelectOrder = UiSelectGeneric<StepOrder>();

const stepsStore = useStepsStore()
const i18n = inject<I18N>('i18n')

const dynamicSize = ref<number| null>(null)

const sizeRanges: Record<StepUnit, [number, number]> = {
    second: [2, 100],
    minute: [1, 60],
    hour: [1, 100],
    day: [1, 100],
    week: [1, 10],
    month: [1, 12],
    year: [1, 10],
}

const defaultSizes = computed(() => {
    const sizeRange = sizeRanges[stepsStore.unit]
    const [start, end] = sizeRange
    const length = end - start + 1
    return Array.from({length}, (_, i) => start + i)
})

const sizeItems = computed<UiSelectItemInterface<number>[]>(() => {
    const sizes = dynamicSize.value ? [dynamicSize.value, ...defaultSizes.value] : defaultSizes.value

    return sizes.map(item => {
        return {
            __type: 'item',
            id: item,
            label: `${item}`,
            value: item,
        }
    })
});

const unitItems = computed<UiSelectItemInterface<StepUnit>[]>(() => {
    return stepUnits.map(item => ({
        __type: 'item',
        id: item,
        label: i18n?.$t(`common.timeUnits.${item}`) ?? item,
        value: item
    }))
})

const orderItems = computed<UiSelectItemInterface<StepOrder>[]>(() => {
    return stepOrders.map(item => ({
        __type: 'item',
        id: item,
        label: i18n?.$t(`criteria.orderType.${item}`) ?? item,
        value: item
    }))
})

const size = computed({
    get(): number {
        return stepsStore.size
    },
    set(value: number) {
        stepsStore.setSize(value)
    }
})

const unit = computed({
    get(): StepUnit {
        return stepsStore.unit
    },
    set(value: StepUnit) {
        stepsStore.setUnit(value)
    }
})

const order = computed({
    get(): StepOrder {
        return stepsStore.order
    },
    set(value: StepOrder) {
        stepsStore.setOrder(value)
    }
})

const handleSizeSearch = (value: string, items: UiSelectItemInterface<number>[]) => {
    dynamicSize.value = null

    const parsedSize = Number(value)
    if (isNaN(parsedSize)) {
        return
    }

    if (defaultSizes.value.includes(parsedSize)) {
        return
    }

    dynamicSize.value = parsedSize
}
</script>
