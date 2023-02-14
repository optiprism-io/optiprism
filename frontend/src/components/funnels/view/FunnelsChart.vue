<template>
    <div class="pf-l-flex pf-u-justify-content-center pf-u-flex-nowrap">
        <div
            v-for="(item, i) in dimensions"
            :key="i"
            class="pf-l-flex pf-l-flex__item pf-m-align-items-center"
        >
            <span
                class="pf-l-flex__item legend-marker"
                :style="{ background: barsColors[i] }"
            />
            <span>{{ item }}</span>
        </div>
    </div>
    <div
        :class="{
            'pf-c-scroll-inner-wrapper': !props.liteChart,
        }"
    >
        <div
            ref="container"
            class="pf-l-flex pf-u-flex-nowrap"
            :class="{
                'pf-u-m-lg': !props.liteChart,
            }"
        >
            <div
                v-for="(_, i) in stepIterator"
                :key="i"
                class="pf-m-flex-1 pf-m-spacer-none"
            >
                <FunnelChartStacked
                    :data="data[i]"
                    :width="stepWidth"
                    :colors="barsColors"
                    :height="props.height"
                    :lite-chart="props.liteChart"
                >
                    <div class="pf-u-text-align-center">
                        {{ stepNames[i] }}
                    </div>
                </FunnelChartStacked>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed, onBeforeUnmount, onMounted, ref, watch} from 'vue'
import FunnelChartStacked from '@/components/funnels/view/FunnelChartStacked.vue';
import {useStepsStore} from '@/stores/funnels/steps';
import {useEventName} from '@/helpers/useEventName';
import { useFunnelsStore, convertColumns } from '@/stores/funnels/funnels'
import { DataTableResponseColumnsInner } from '@/api'
import { Step } from '@/types/steps'

const container = ref<HTMLDivElement | null>(null)
const containerWidth = ref(0)

const funnelsStore = useFunnelsStore();
const eventName = useEventName()
const stepsStore = useStepsStore()

type Props = {
    liteChart?: boolean
    reports?: DataTableResponseColumnsInner[]
    steps?: Step[]
    minWidthStep?: number
    height?: number
    width?: number
}

const props = withDefaults(defineProps<Props>(), {
    liteChart: false,
    minWidthStep: 550,
    height: 500,
})

const reports = computed(() => props.reports ?? funnelsStore.reports)

const stepNumbers = computed((): number[] => {
    const metricValueColumns = reports.value.filter(col => col.type === 'funnelMetricValue')
    const stepNumbers = metricValueColumns.map(col => col.step) as number[]
    return [...new Set(stepNumbers)]
})

const dimensions = computed((): string[] => {
    const result: string[] = []
    const columns = reports.value.filter(col => col.type === 'dimension')

    for (let i = 0; i < (columns[0]?.data?.length ?? 0); i++) {
        const row: string[] = []
        columns.forEach(item => {
            row.push(`${item.data?.[i] ?? ''}`)
        })
        result.push(row.join(' / '))
    }

    return result
})

const conversionCount = computed((): number[][] => {
    const columns = reports.value.filter(col => col.name === 'conversionCount')
    return convertColumns(columns, stepNumbers.value)
})

const conversionRatio = computed((): number[][] => {
    const columns = reports.value.filter(col => col.name === 'conversionRatio')
    return convertColumns(columns, stepNumbers.value)
})

const dropOffCount = computed((): number[][] => {
    const columns = reports.value.filter(col => col.name === 'dropOffCount')
    return convertColumns(columns, stepNumbers.value)
})

const dropOffRatio = computed((): number[][] => {
    const columns = reports.value.filter(col => col.name === 'dropOffRatio')
    return convertColumns(columns, stepNumbers.value)
})

const colors = ['#ee5253', '#2e86de', '#ff9f43', '#5f27cd', '#10ac84', '#f368e0', '#0abde3']
const barsColors = computed(() => {
    return colors.slice(0, dimensions.value.length)
})

const stepWidth = computed(() => {
    return Math.max(containerWidth.value / stepIterator.value.length, props.minWidthStep)
})

const stepIterator = computed(() => {
    return Array.from({
        length: Math.min(stepNames.value.length, stepNumbers.value.length)
    })
})

const steps = computed(() => props.steps ?? stepsStore.steps)

const stepNames = computed<string[]>(() => {
    const events = steps.value.map(step => step.events.map(event => event.event))
    return events.map(items => {
        return items.map(eventName).join(' or ')
    })
})

const data = computed(() => {
    return Array.from({ length: stepNumbers.value.length }).map((_, i) => {
        return Array.from({ length: dimensions.value.length }).map((_, j) => {
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

watch(() => props.width, (width) => {
    if (width) {
        containerWidth.value = width - 1;
    }
});

const observer = ref<ResizeObserver | null>(null)

onMounted(() => {
    observer.value = new ResizeObserver(onResize)
    if (container.value) {
        observer.value.observe(container.value)
    }
    window.addEventListener('resize', onResize)
})

onBeforeUnmount(() => {
    window.removeEventListener('resize', onResize)
})
</script>

<style lang="scss" scoped>
.legend-marker {
    width: 1rem;
    height: 1rem;
    border-radius: .125rem;
}
</style>
