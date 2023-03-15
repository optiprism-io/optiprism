<template>
    <nav
        class="pf-c-nav pf-m-horizontal-subnav"
        aria-label="Local"
    >
        <ul class="pf-c-nav__list">
            <li
                v-for="item in items"
                :key="item.name"
                class="pf-c-nav__item"
            >
                <router-link
                    :to="item.to"
                    class="pf-c-nav__link"
                    :class="{
                        'pf-m-current': item.active
                    }"
                    aria-current="page"
                >
                    {{ item.name }}
                </router-link>
            </li>
        </ul>
    </nav>
</template>

<script lang="ts" setup>
import { computed, inject } from 'vue'
import { useRoute } from 'vue-router'
import { pagesMap } from '@/router'

const i18n = inject<any>('i18n')
const route = useRoute()

const configNav = [
    {
        name: 'dashboards.title',
        to: pagesMap.dashboards.name,
        activeKey: 'dashboards',
    },
    {
        name: 'reports.title',
        to: pagesMap.reportsEventSegmentation.name,
        activeKey: 'reports',
    },
    {
        name: 'events.events',
        to: pagesMap.eventsLiveStream.name,
        activeKey: 'events',
    },
    {
        name: 'users.title',
        to: 'users',
        activeKey: 'users',
    },
]

const items = computed(() => {
    return configNav.map(item => {
        return {
            name: i18n.$t(item.name),
            to: {
                name: item.to,
            },
            active: route.name === item.to || (typeof route.name === 'string' && route.name.includes(item.activeKey)),
        }
    })
})
</script>

<style scoped>
.pf-c-nav__link.pf-m-current,
.pf-c-nav__link.pf-m-current:hover,
.pf-c-nav__item.pf-m-current:not(.pf-m-expanded) .pf-c-nav__link {
    color: var(--pf-c-nav__link--m-current--Color);
    background-color: var(--pf-global--palette--cyan-700);
}
</style>