<template>
    <div class="pf-l-flex pf-m-column pf-u-m-lg">
        <div
            ref="container"
            class="pf-l-flex__item"
        />
        <div class="pf-u-font-size-xl pf-u-font-weight-bold pf-u-text-align-center pf-l-flex__item">
            <slot />
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, PropType, ref, watch} from 'vue';
import {Chart} from '@antv/g2';
import { lighten } from '@/helpers/colorHelper';

const colors = ['#00b894', '#00cec9', '#0984e3', '#6c5ce7', '#e17055', '#fdcb6e']
const container = ref<HTMLDivElement | null>(null)

const props = defineProps({
    data: {
        type: Array as PropType<Record<string, string | number>[]>,
        default: () => []
    },
    xVal: {
        type: String,
        required: true
    },
    yVals: {
        type: Object as PropType<Record<string, string>>,
        required: true
    },
    labels: {
        type: Array as PropType<string[]>,
        default: () => []
    },
    reverseY: {
        type: Boolean,
        default: false
    }
})

const yAxisVals = computed(() => {
    return Object.keys(props.yVals)
})

const dataView = computed(() => {
    const reservedColors = colors.slice(0, props.data.length)
    const values = props.reverseY ? [...yAxisVals.value].reverse() : yAxisVals.value

    return props.data
        .map((item, i) => {
            return values.map((key, j) => {
                const iterator = props.reverseY ? values.length - j - 1 : j
                return {
                    key,
                    [props.xVal]: item[props.xVal],
                    total: item[key],
                    color: lighten(reservedColors[i], iterator * 50)
                }
            })
        })
        .flat()
})

watch(() => [container.value, dataView.value], () => {
    if (!container.value) {
        return
    }

    const chart = new Chart({
        container: container.value,
        autoFit: true,
        height: 500,
    });

    chart
        .legend(false)
        .data(dataView.value)
        .scale('total', { nice: true })
        .axis('dimension', false)
        .axis('total', false)
        .interval({ intervalPadding: 20 })
        .adjust('stack')
        .position('dimension*total')
        .color('color', color => color)
        .tooltip('key*total', (key, total) => {
            return {
                name: props.yVals[key],
                value: total
            }
        });

    chart.render();
})
</script>
