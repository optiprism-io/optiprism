<template>
    <UiTabs
        class="pf-u-mb-md"
        :items="items"
    />
</template>

<script lang="ts" setup>
import { computed, inject } from 'vue'
import { useRoute } from 'vue-router'
const i18n = inject<any>('i18n')

const route = useRoute()

const items = computed(() => {
    const mapTabs = [
        {
            name: i18n.$t('events.live_stream.title'),
            value: 'events_live_stream',
            link: '/',
            icon: 'fas fa-chart-pie'
        },
        {
            name: i18n.$t('events.events'),
            value: 'events',
            link: '/events',
            icon: 'pf-icon pf-icon-filter'
        },
    ];

    return mapTabs.map(item => {
        return {
            ...item,
            active: route.name === item.value,
        }
    })
})
</script>