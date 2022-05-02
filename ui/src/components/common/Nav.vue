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
                    :to="item.link"
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
const i18n = inject<any>('i18n')
const route = useRoute()

const items = computed(() => {
    const mapTabs = [
        {
            name: i18n.$t('events.events'),
            value: 'events.events',
            link: '/',
            active: typeof route.name === 'string' && route.name.includes('events'),
        },
        {
            name: i18n.$t('users.title'),
            value: 'users',
            link: '/users',
        },
    ]

    return mapTabs.map(item => {
        return {
            ...item,
            active: route.name === item.value || item.active,
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