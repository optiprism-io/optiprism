<template>
    <section class="pf-c-page__main-section">
        <UiTabs
            class="pf-u-mb-md"
            :items="items"
        />
        <router-view />
    </section>
</template>

<script lang="ts" setup>
import { computed } from "vue";
import { useRoute } from "vue-router";
import UiTabs from "@/components/uikit/UiTabs.vue";

const route = useRoute()

const items = computed(() => {
    const mapTabs = [
        {
            name: "Events Segmentation",
            value: "dashboard_events_segmentation",
            link: "/",
            icon: "fas fa-chart-pie"
        },
        {
            name: "Funnels",
            value: "dashboard_funnels",
            link: "/funnels",
            icon: "pf-icon pf-icon-filter"
        },
    ];

    return mapTabs.map(item => {
        return {
            ...item,
            active: route.name === item.value,
        };
    });
});
</script>

<style lang="scss">
</style>