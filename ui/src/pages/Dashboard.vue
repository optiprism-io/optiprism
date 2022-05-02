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
import { computed, inject } from 'vue'
import { useRoute } from 'vue-router';
import UiTabs from '@/components/uikit/UiTabs.vue';
const i18n = inject<any>('i18n')

const route = useRoute()

const items = computed(() => {
    const mapTabs = [
        {
            name: i18n.$t('events.live_stream.title'),
            value: 'dashboard_events_live_stream',
            link: '/',
            icon: 'fas fa-chart-pie'
        },
        {
            name: i18n.$t('events.events'),
            value: 'dashboard_events',
            link: '/events',
            icon: 'pf-icon pf-icon-filter'
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