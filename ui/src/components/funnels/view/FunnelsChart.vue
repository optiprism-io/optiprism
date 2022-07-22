<template>
    <div class="pf-c-scroll-inner-wrapper">
        <div
            ref="container"
            class="pf-l-flex pf-u-flex-nowrap pf-u-m-lg"
        >
            <div
                v-for="(_, i) in stepIterator"
                :key="i"
                class="pf-m-flex-1 pf-m-spacer-none"
            >
                <FunnelChartStacked
                    :data="data[i]"
                    :width="stepWidth"
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
import {useFunnelsStore} from '@/stores/funnels/funnels';

const container = ref<HTMLDivElement | null>(null)
const containerWidth = ref(0)

const funnelsStore = useFunnelsStore();
const eventName = useEventName()
const stepsStore = useStepsStore()

const stepWidth = computed(() => {
    return Math.max(containerWidth.value / stepIterator.value.length, 550)
})

const stepIterator = computed(() => {
    return Array.from({
        length: Math.min(stepNames.value.length, funnelsStore.stepNumbers.length)
    })
})

const stepNames = computed<string[]>(() => {
    const events = stepsStore.steps.map(step => step.events.map(event => event.event))
    return events.map(items => {
        return items.map(eventName).join(' or ')
    })
})

const data = computed(() => {
    return Array.from({ length: funnelsStore.stepNumbers.length }).map((_, i) => {
        return Array.from({length: funnelsStore.dimensions.length}).map((_, j) => {
            return {
                dimension: funnelsStore.dimensions[j],
                conversionCount: funnelsStore.conversionCount[i]?.[j] ?? 0,
                conversionRatio: funnelsStore.conversionRatio[i]?.[j] ?? 0,
                dropOffCount: funnelsStore.dropOffCount[i]?.[j] ?? 0,
                dropOffRatio: funnelsStore.dropOffRatio[i]?.[j] ?? 0,
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
