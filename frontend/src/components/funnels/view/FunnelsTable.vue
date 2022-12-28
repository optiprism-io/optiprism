<template>
    <div class="pf-c-scroll-inner-wrapper">
        <UiTable
            :items="data"
            :columns="columns"
            :groups="columnGroups"
        />
    </div>
</template>

<script lang="ts" setup>
import {computed, inject} from 'vue';
import {Column, ColumnGroup, Row} from '@/components/uikit/UiTable/UiTable';
import {I18N} from '@/utils/i18n';
import {useEventName} from '@/helpers/useEventName';
import {useStepsStore} from '@/stores/funnels/steps';
import { DataTableResponseColumnsInner } from '@/api';
import { useFunnelsStore } from '@/stores/funnels/funnels';

const { $t } = inject('i18n') as I18N

const funnelsStore = useFunnelsStore();
const stepsStore = useStepsStore()
const eventName = useEventName()

const stepNames = computed<string[]>(() => {
    const events = stepsStore.steps.map(step => step.events.map(event => event.event))
    return events.map(items => {
        return items.map(eventName).join(' or ')
    })
})

const stepIterator = computed(() => {
    return Array.from({
        length: Math.min(stepNames.value.length, funnelsStore.stepNumbers.length)
    })
})

const columnGroups = computed<ColumnGroup[]>(() => {
    return [
        {
            title: '',
            value: 'dimensions',
            span: dimensions.value.length + 1,
            fixed: true,
            lastFixed: true,
        },
        ...stepIterator.value.map((item, i) => {
            return {
                title: stepNames.value[i],
                value: `step${i + 1}`,
                span: funnelMetricValues.value[i].length,
                lastFixed: true
            }
        })
    ]
})

const dimensions = computed(() => {
    return funnelsStore.reports.filter(col => col.type === 'dimension')
})

const totalDimensions = computed(() => dimensions.value[0]?.data?.length ?? 0)

const funnelMetricValues = computed(() => {
    const res = funnelsStore.reports
        .filter(col => col.type === 'funnelMetricValue')
        .reduce((result, col) => {
            if (!col.step) {
                return result
            }

            if (result[col.step]) {
                result[col.step].push(col)
            } else {
                result[col.step] = [col]
            }

            return result
        }, {} as Record<string | number, DataTableResponseColumnsInner[]>)

    const columns: DataTableResponseColumnsInner[][] = []

    Object.keys(res).sort().forEach(key => {
        columns.push(res[key])
    })

    return columns.slice(0, stepIterator.value.length)
})

const dimensionColumns = computed<Column[]>(() => {
    return dimensions.value.map((item, i) => {
        return {
            title: item.name ?? '',
            value: `dimension-${i + 1}`,
            fixed: true
        }
    })
})

const funnelMetricValueColumns = computed<Column[]>(() => {
    return funnelMetricValues.value.map(grp => {
        return grp.map((item, i) => {
            return {
                title: $t(`funnels.view.${item.name}`),
                value: item.name ?? '',
                lastFixed: i === grp.length - 1,
            }
        })
    }).flat()
})

const funnelMetricValueValues = computed(() => {
    return Array.from({ length: totalDimensions.value }).map((_, i) => {
        return funnelMetricValues.value.map(grp => {
            return grp.map((item, j) => {
                const postfix = item.name?.indexOf('Ratio') !== -1 ? '%' : ''
                return {
                    title: item.data ? item.data[i] + postfix : '',
                    lastFixed: j === grp.length - 1,
                }
            })
        }).flat()
    })
})

const columns = computed<Column[]>(() => [
    ...dimensionColumns.value,
    ...(
        funnelsStore.reports.length > 0
            ? [{
                title: $t('funnels.view.totalConversionRatio'),
                value: 'totalConversionRatio',
                fixed: true,
                lastFixed: true,
            }]
            : []
    ),
    ...funnelMetricValueColumns.value
])

const data = computed<Row[]>(() => {
    const totalConversionRatio = funnelsStore.reports
        ?.find(col => col.name === 'totalConversionRatio')
        ?.data?.map(item => `${item}%`)
      ?? Array.from({ length: totalDimensions.value }).map(() => '0%')

    return Array.from({ length: totalDimensions.value }).map((_, i) => {
        return [
            ...dimensions.value.map(item => {
                return {
                    title: item.data?.[i] ?? '',
                    fixed: true,
                }
            }),
            {
                title: totalConversionRatio[i],
                fixed: true,
                lastFixed: true
            },
            ...funnelMetricValueValues.value[i]
        ] as Row
    })
})
</script>
