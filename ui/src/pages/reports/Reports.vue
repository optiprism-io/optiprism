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
import { useRoute } from 'vue-router'
const i18n = inject<any>('i18n')
const route = useRoute()

const items = computed(() => {
    const mapTabs = [
        {
            name: i18n.$t('events.event_segmentation'),
            value: 'reports_event_segmentation',
            link: {
                name: 'reports_event_segmentation'
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
</script>
