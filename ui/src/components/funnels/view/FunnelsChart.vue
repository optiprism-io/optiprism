<template>
    <div class="pf-l-flex">
        <div
            v-for="(item, i) in dat"
            :key="i"
            class="pf-l-flex__item pf-m-flex-1"
        >
            <ChartStacked
                :x-val="'dimension'"
                :y-vals="{
                    'conversionCount': 'Conversion Count',
                    'dropOffCount': 'Drop Off Count',
                }"
                :data="item"
                reverse-y
            />
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, ref} from 'vue'
import ChartStacked from '@/components/charts/stacked/ChartStacked.vue';

const container = ref<HTMLDivElement | null>(null)

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

const conversionCount = computed(() => {
    const result: number[][] = []
    const columns = json.columns.filter(col => col.name === 'conversionCount')

    for (let i = 0; i < stepNumbers.value.length; i++) {
        const column = columns.find(col => col.step === stepNumbers.value[i])
        if (column) {
            result.push(column.values as number[])
        } else {
            result.push([])
        }
    }

    return result
})

const dropOffCount = computed(() => {
    const result: number[][] = []
    const columns = json.columns.filter(col => col.name === 'dropOffCount')

    for (let i = 0; i < stepNumbers.value.length; i++) {
        const column = columns.find(col => col.step === stepNumbers.value[i])
        if (column) {
            result.push(column.values as number[])
        } else {
            result.push([])
        }
    }

    return result
})

const dat = computed(() => {
    return Array.from({length: stepNumbers.value.length}).map((_, i) => {
        return Array.from({length: dimensions.value.length}).map((_, j) => {
            return {
                dimension: dimensions.value[j],
                conversionCount: conversionCount.value[i]?.[j] ?? 0,
                dropOffCount: dropOffCount.value[i]?.[j] ?? 0,
            }
        })
    })
})
</script>
