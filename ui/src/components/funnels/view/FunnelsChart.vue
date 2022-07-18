<template>
    <div
        ref="container"
        class="pf-l-flex pf-u-m-lg"
    >
        <div
            v-for="(_, i) in iterator"
            :key="i"
            class="pf-m-flex-1 pf-m-spacer-none"
        >
            <ChartStacked
                :data="data[i]"
                :x-key="'dimension'"
                :y-keys="[
                    ['conversionCount', 'conversionRatio'],
                    ['dropOffCount', 'dropOffRatio']
                ]"
                :labels="{
                    'conversionCount': $t('funnels.chart.conversionCount'),
                    'dropOffCount': $t('funnels.chart.dropOffCount'),
                    'conversionRatio': $t('funnels.chart.conversionRatio'),
                    'dropOffRatio': $t('funnels.chart.dropOffRatio'),
                }"
                :width="stepWidth"
            >
                <div class="pf-l-flex pf-m-nowrap">
                    <div class="pf-l-flex__item pf-u-color-400">
                        {{ stepNumbers[i] }}
                    </div>
                    <div class="pf-l-flex__item">
                        {{ stepNames[i] }}
                    </div>
                </div>
            </ChartStacked>
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, onBeforeUnmount, onMounted, ref, watch} from 'vue'
import ChartStacked from '@/components/charts/stacked/ChartStacked.vue';
import {useStepsStore} from '@/stores/funnels/steps';
import {useEventName} from '@/helpers/useEventName';

const container = ref<HTMLDivElement | null>(null)
const containerWidth = ref(0)

const json = {
    'columns': [
        {
            'type': 'dimension',
            'dataType': 'string',
            'name': 'Login/Signup',
            'values': [
                'Login',
                'Login',
                'Signup',
                'Signup'
            ]
        },
        {
            'type': 'dimension',
            'dataType': 'string',
            'name': 'Platform',
            'values': [
                'Android',
                'iOS',
                'Android',
                'iOS'
            ]
        },
        {
            'type': 'metricValue',
            'dataType': 'number',
            'name': 'totalConversionRatio',
            'values': [
                46.93,
                44.80,
                48.96,
                42.67
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 1,
            'dataType': 'number',
            'name': 'conversionCount',
            'values': [
                10016,
                8803,
                3519,
                3403
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'conversionCount',
            'values': [
                8670,
                7448,
                2763,
                2548
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'dropOffCount',
            'values': [
                1346,
                1355,
                756,
                855
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'dropOffRatio',
            'values': [
                13.44,
                15.39,
                21.48,
                25.12
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'dropOffRatioFromFirstStep',
            'values': [
                13.44,
                15.39,
                21.48,
                25.12
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'avgTimeFromPrevStep',
            'values': [
                146880,
                164160,
                518400,
                588400
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'avgTimeFromFirstStep',
            'values': [
                146880,
                164160,
                518400,
                588400
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'conversionRatio',
            'values': [
                86.56,
                84.61,
                78.52,
                74.88
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 2,
            'dataType': 'number',
            'name': 'conversionRatioFromFirstStep',
            'values': [
                86.56,
                84.61,
                78.52,
                74.88
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'conversionCount',
            'values': [
                4701,
                3944,
                1723,
                1452
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'dropOffCount',
            'values': [
                3969,
                3504,
                1040,
                1096
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'dropOffRatio',
            'values': [
                45.78,
                47.01,
                37.64,
                43.01
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'dropOffRatioFromFirstStep',
            'values': [
                13.44,
                15.39,
                21.48,
                25.12
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'avgTimeFromPrevStep',
            'values': [
                117279,
                114160,
                468400,
                518400
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'avgTimeFromFirstStep',
            'values': [
                827837,
                264160,
                618400,
                688400
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'conversionRatio',
            'values': [
                54.22,
                52.95,
                62.36,
                59.99
            ]
        },
        {
            'type': 'funnelMetricValue',
            'step': 3,
            'dataType': 'number',
            'name': 'conversionRatioFromFirstStep',
            'values': [
                46.93,
                44.80,
                48.96,
                42.67
            ]
        }
    ]
}

const eventName = useEventName()
const stepsStore = useStepsStore()

const stepWidth = computed(() => {
    return containerWidth.value / iterator.value.length
})

const iterator = computed(() => {
    return Array.from({
        length: Math.min(stepNames.value.length, stepNumbers.value.length)
    })
})

const stepNames = computed<string[]>(() => {
    const events = stepsStore.steps.map(step => step.events.map(event => event.event))
    return events.map(items => {
        return items.map(eventName).join(' or ')
    })
})

const stepNumbers = computed<number[]>(() => {
    const metricValueColumns = json.columns.filter(col => col.type === 'funnelMetricValue')
    const stepNumbers = metricValueColumns.map(col => col.step) as number[]
    return [...new Set(stepNumbers)]
})

const dimensions = computed(() => {
    const result: string[] = []
    const columns = json.columns.filter(col => col.type === 'dimension')

    for (let i = 0; i < columns[0].values.length; i++) {
        const row: string[] = []
        columns.forEach(item => {
            row.push(`${item.values[i]}`)
        })
        result.push(row.join(' / '))
    }

    return result
})

const convertColumn = (columns: typeof json.columns) => {
    const result: number[][] = []

    for (let i = 0; i < stepNumbers.value.length; i++) {
        const column = columns.find(col => col.step === stepNumbers.value[i])
        if (column) {
            result.push(column.values as number[])
        } else {
            result.push([])
        }
    }

    return result
}

const conversionCount = computed(() => {
    const columns = json.columns.filter(col => col.name === 'conversionCount')
    return convertColumn(columns)
})


const conversionRatio = computed(() => {
    const columns = json.columns.filter(col => col.name === 'conversionRatio')
    return convertColumn(columns)
})

const dropOffCount = computed(() => {
    const columns = json.columns.filter(col => col.name === 'dropOffCount')
    return convertColumn(columns)
})

const dropOffRatio = computed(() => {
    const columns = json.columns.filter(col => col.name === 'dropOffRatio')
    return convertColumn(columns)
})

const data = computed(() => {
    return Array.from({length: stepNumbers.value.length}).map((_, i) => {
        return Array.from({length: dimensions.value.length}).map((_, j) => {
            return {
                dimension: dimensions.value[j],
                conversionCount: conversionCount.value[i]?.[j] ?? 0,
                conversionRatio: conversionRatio.value[i]?.[j] ?? 0,
                dropOffCount: dropOffCount.value[i]?.[j] ?? 0,
                dropOffRatio: dropOffRatio.value[i]?.[j] ?? 0,
            }
        })
    })
})

const onResize = () => {
    if (container.value) {
        containerWidth.value = container.value.clientWidth - 1
    }
}

watch(container, onResize)

onMounted(() => {
    window.addEventListener('resize', onResize)
})

onBeforeUnmount(() => {
    window.removeEventListener('resize', onResize)
})
</script>
