<template>
    <nav
        class="pf-c-nav pf-m-horizontal-subnav"
        aria-label="Local"
    >
        <Select
            :grouped="true"
            :items="itemsDashboards"
            :cloase-after-action="true"
            @select="changeDashboard"
            @action="selectAction"
        >
            <UiButton
                class="pf-u-color-light-100 pf-u-h-100"
                :after-icon="'fas fa-caret-down'"
            >
                {{ $t('dashboards.title') }}
            </UiButton>
        </Select>
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
import { useRoute, useRouter } from 'vue-router'
import { pagesMap } from '@/router'
import Select from '@/components/Select/Select.vue';
import { useDashboardsStore } from '@/stores/dashboards';

const i18n = inject<any>('i18n')
const route = useRoute()
const router = useRouter()
const dashboardsStore = useDashboardsStore()

const configNav = [
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
        name: i18n.$t('users.title'),
        to: 'users',
        activeKey: 'users',
    },
]

const dashboards = computed(() => dashboardsStore.dashboards)

const selectDashboards = computed(() => {
    return dashboards.value.map(item => {
        const id = Number(item.id)
        return {
            value: id,
            key: id,
            nameDisplay: item.name || '',
        }
    });
})

const activeDashboardId = computed(() => {
    return route.name === pagesMap.dashboards.name && route?.query?.id ? Number(route.query.id) : 0;
});

const itemsDashboards = computed(() => {
    return [
        {
            action: {
                icon: 'fas fa-plus-circle',
                text: 'common.create',
                type: 'createEvent',
            },
            items: dashboards.value.map(item => {
                const id = Number(item.id)
                return {
                    description: item.description,
                    name: item.name || '',
                    selected: id === activeDashboardId.value,
                    item: {
                        id,
                        type: id,
                    },
                };
            }),
            name: 'Dashboards',
            type: 'dashboards',
        },
    ];
});

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

const changeDashboard = (payload: { id: number }) => {
    router.push({
        name: pagesMap.dashboards.name,
        query: { id: payload.id }
    })
}

const selectAction = (payload: string) => {
    if (payload === 'createEvent') {
        router.push({
            name: pagesMap.dashboards.name,
            query: {
                new: 1,
                id: null,
            },
        })
    }
}
</script>

<style scoped>
.pf-c-nav__link.pf-m-current,
.pf-c-nav__link.pf-m-current:hover,
.pf-c-nav__item.pf-m-current:not(.pf-m-expanded) .pf-c-nav__link {
    color: var(--pf-c-nav__link--m-current--Color);
    background-color: var(--pf-global--palette--cyan-700);
}
</style>