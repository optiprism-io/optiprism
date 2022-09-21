<template>
    <section class="pf-c-page__main-section">
        <UiTabs
            class="pf-u-mb-md"
            :items="items"
        />
        <div
            v-if="reportsStore.loading"
            class="pf-u-h-66vh pf-u-display-flex pf-u-align-items-center pf-u-justify-content-center"
        >
            <UiSpinner :size="'xl'" />
        </div>
        <router-view v-else />
    </section>
</template>

<script lang="ts" setup>
import {computed, inject, onMounted} from 'vue'
import { useRoute } from 'vue-router'
import { pagesMap } from '@/router'
import { useLexiconStore } from '@/stores/lexicon'
import { useEventsStore } from '@/stores/eventSegmentation/events'
import { useReportsStore } from '@/stores/reports/reports'

import UiSpinner from '@/components/uikit/UiSpinner.vue'

const i18n = inject<any>('i18n')
const route = useRoute()

const items = computed(() => {
    const mapTabs = [
        {
            name: i18n.$t('events.event_segmentation'),
            value: pagesMap.reportsEventSegmentation.name,
            link: {
                name: pagesMap.reportsEventSegmentation.name,
            },
            icon: 'pf-icon pf-icon-filter'
        },
        {
            name: i18n.$t('funnels.funnels'),
            value: 'reports_funnels',
            link: {
                name: 'reports_funnels'
            },
            icon: 'pf-icon pf-icon-filter'
        }
    ];

    return mapTabs.map(item => {
        return {
            ...item,
            active: route.name === item.value,
        }
    })
})

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();
const reportsStore = useReportsStore()

onMounted(async () => {
    await lexiconStore.getEvents();
    await lexiconStore.getEventProperties();
    await lexiconStore.getUserProperties();

    await eventsStore.initPeriod();

    reportsStore.getList()
});
</script>
