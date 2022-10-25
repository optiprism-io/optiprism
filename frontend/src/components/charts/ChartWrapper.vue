<template>
    <div class="chart-wrapper">
        <div
            v-if="loading"
            class="chart-wrapper__spinner"
        >
            <UiSpinner :size="'xl'" />
        </div>
        <div
            ref="chart"
            class="chart-wrapper__container"
        />
    </div>
</template>


<script setup lang="ts">
import { ref, onMounted, watch } from 'vue';
import merge from 'lodash/merge';
import UiSpinner from '../uikit/UiSpinner.vue';

const props = withDefaults(
    defineProps<{
        options: any
        loading?: boolean
        defaultOptions?: any
        chartConstructor: any
        height?: string
    }>(),
    {
        options: {},
        defaultOptions: {},
        loading: false,
        height: '350px',
    }
);

const chart = ref<HTMLInputElement | null>(null)
const chartLib = ref()

const deleteReactivity = (data: any) => {
    return JSON.parse(JSON.stringify(data));
};

const updateOptions = () => {
    const lineChartContainer: any = chart.value;
    const options = merge(deleteReactivity(props.defaultOptions), deleteReactivity(props.options));

    if (chartLib.value) {
        chartLib.value.update(options);
    } else {
        chartLib.value = new props.chartConstructor(lineChartContainer, options);
        chartLib.value.render();
    }
};

watch(
    () => props.options,
    () => {
        setTimeout(updateOptions);
    }
);

onMounted(() => {
    updateOptions();
});
</script>

<style lang="scss">
.chart-wrapper {
    position: relative;
    box-sizing: border-box;
    height: v-bind('props.height');

    &__spinner {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
        background-color: rgba(#fff, .8);
        z-index: 2;
    }
}
</style>