<template>
    <div class="pf-l-flex pf-m-column">
        <div
            ref="container"
            class="pf-l-flex__item"
        />
        <div class="pf-u-font-size-lg pf-u-font-weight-bold pf-l-flex__item pf-u-px-lg">
            <slot />
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, PropType, ref, watch} from 'vue';
import {Chart} from '@antv/g2';
import { lighten } from '@/helpers/colorHelper';

const container = ref<HTMLDivElement | null>(null)
const chart = ref<Chart | null>(null)

const props = defineProps({
    data: {
        type: Array as PropType<Record<string, string | number>[]>,
        default: () => []
    },
    xKey: {
        type: String,
        required: true
    },
    yKeys: {
        type: Array as PropType<string[][]>,
        required: true
    },
    labels: {
        type: Object as PropType<Record<string, string>>,
        default: () => ({})
    },
    colors: {
        type: Array as PropType<string[]>,
        default: () => ['#ee5253', '#2e86de', '#ff9f43', '#5f27cd', '#10ac84', '#f368e0', '#0abde3']
    },
    width: {
        type: Number,
        default: 400
    },
})

const primaryKeys = computed(() => props.yKeys.map(keys => keys[0]))
const secondaryKeys = computed(() => props.yKeys.map(keys => keys[1]))

const dataView = computed(() => {
    const colors = props.colors.slice(0, props.data.length)

    return props.data
        .map((item, i) => {
            return Array.from({ length: primaryKeys.value.length }).map((_, j) => {
                const iterator = primaryKeys.value.length - 1 - j

                const primaryKey = primaryKeys.value[iterator]
                const secondaryKey = secondaryKeys.value[iterator]

                return {
                    [props.xKey]: item[props.xKey],
                    primaryKey,
                    secondaryKey,
                    primaryValue: item[primaryKey],
                    secondaryValue: item[secondaryKey],
                    color: lighten(colors[i], iterator * 80),
                }
            })
        })
        .flat()
})

watch(() => [container.value, dataView.value], () => {
    if (!container.value) {
        return
    }

    if (chart.value) {
        chart.value.destroy()
    }

    chart.value = new Chart({
        container: container.value,
        height: 500,
        width: props.width,
        autoFit: false,
        padding: 20,
    });

    chart.value
        .animate(false)
        .legend(false)
        .tooltip({
            customItems: originalItems => {
                const [item] = originalItems
                const { primaryKey, secondaryKey, primaryValue, secondaryValue } = item.data as Record<string, string>

                const secondaryBlock = secondaryKey && secondaryValue
                    ? [{
                        ...item,
                        name: props.labels[secondaryKey] ?? secondaryKey,
                        value: secondaryValue
                    }]
                    : []

                return [
                    {
                        ...item,
                        name: props.labels[primaryKey] ?? primaryKey,
                        value: primaryValue,
                    },
                    ...secondaryBlock
                ]
            }
        })
        .data(dataView.value)
        .axis('dimension', false)
        .axis('primaryValue', false)
        .interval({ intervalPadding: 20 })
        .adjust('stack')
        .position('dimension*primaryValue')
        .color('color', color => color)

    chart.value.render();
})

watch(() => props.width, (width) => {
    if (chart.value) {
        chart.value.changeSize(width, 500)
    }
}, {immediate: true})
</script>
