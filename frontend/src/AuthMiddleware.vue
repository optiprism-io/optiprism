<template>
    <div
        v-if="state === 'ok'"
        class="pf-c-page"
    >
        <Header />
        <main class="pf-c-page__main">
            <router-view />
        </main>

        <CreateCustomEvent
            v-if="commonStore.showCreateCustomEvent"
            @apply="applyCreateCustomEvent"
            @cancel="togglePopupCreateCustomEvent(false)"
        />
    </div>
</template>

<script setup lang="ts">
import Header from '@/components/common/Header.vue'
import CreateCustomEvent from '@/components/events/CreateCustomEvent.vue'
import { useCommonStore } from '@/stores/common'
import { onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth/auth'
import { useDashboardsStore } from '@/stores/dashboards'
import { useReportsStore } from '@/stores/reports/reports'
import { pagesMap } from '@/router'

const state = ref<'pending' | 'ok' | 'error'>('pending')

const route = useRoute()
const router = useRouter()
const commonStore = useCommonStore()
const authStore = useAuthStore()
const dashboardsStore = useDashboardsStore()
const reportsStore = useReportsStore()

const togglePopupCreateCustomEvent = (payload: boolean) => {
    commonStore.togglePopupCreateCustomEvent(payload)
}

const applyCreateCustomEvent = () => {
    togglePopupCreateCustomEvent(false)
}

const getStartData = () => {
    if (!dashboardsStore.dashboards.length) {
        dashboardsStore.getDashboards();
    }
    if (!reportsStore.list.length) {
        reportsStore.getList();
    }
};

const init = async (): Promise<void> => {
    await authStore.authAccess()

    if (!authStore.isAuthenticated) {
        await router.replace({
            name: pagesMap.login.name,
            query: { next: route.path }
        })
        return Promise.resolve()
    } else {
        getStartData();
        state.value = 'ok'
    }
}

onMounted(init)

watch(() => authStore.isAuthenticated, isAuthenticated => {
    if (!isAuthenticated) {
        router.replace({ name: pagesMap.login.name })
    } else {
        state.value = 'ok'
    }
})
</script>
