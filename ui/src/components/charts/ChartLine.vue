<template>
    <div class="chart-line">
        <ChartWrapper
            :loading="props.loading"
            :default-options="defaultOptions"
            :options="props.options"
            :chart-constructor="Line"
        />
    </div>
</template>

<script setup lang="ts">
import { Line } from '@antv/g2plot';
import defaultOptions from "@/configs/chartOptions";
import ChartWrapper from "@/components/charts/ChartWrapper.vue";

const props = withDefaults(
    defineProps<{
        options: any;
        loading: boolean,
    }>(),
    {
        options: {},
        loading: false,
    }
);

</script>

<style lang="scss"></style>