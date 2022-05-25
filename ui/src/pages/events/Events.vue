<template>
    <section class="pf-c-page__main-section">
        <UiTabs
            class="pf-u-mb-md"
            :items="items"
        />
        <router-view />
    </section>
</template>

<script setup lang="ts">
import { computed, inject, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { useLexiconStore } from '@/stores/lexicon'
import { useLiveStreamStore } from '@/stores/reports/liveStream'

const i18n = inject<any>('i18n')
const route = useRoute()
const lexiconStore = useLexiconStore()
const liveStreamStore = useLiveStreamStore()

const items = computed(() => {
    const mapTabs = [
        {
            name: i18n.$t('events.live_stream.title'),
            value: 'events_live_stream',
            link: {
                name: 'events_live_stream',
            },
            icon: 'fas fa-chart-pie'
        },
        {
            name: i18n.$t('events.events'),
            value: 'events_event_management',
            link: {
                name: 'events_event_management',
            },
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

onMounted(async () => {
    liveStreamStore.getReportLiveStream()
    await lexiconStore.getEvents()
    await lexiconStore.getEventProperties()
    await lexiconStore.getUserProperties()
})
</script>

<style scoped lang="scss">
</style>
