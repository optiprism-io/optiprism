<template>
    <router-view />
    <UiAlertGroup
        v-if="alertsStore.items.length"
        class="app-toast-alerts"
        :items="alertsStore.items"
        @close="closeAlert"
    />
</template>

<script lang="ts" setup>
import { inject } from 'vue'
import axios from 'axios'
import { useAuthStore } from '@/stores/auth/auth'
import { useAlertsStore } from '@/stores/alerts'
import { ErrorResponse } from '@/api'
import { I18N } from '@/utils/i18n'
import { useRoute, useRouter } from 'vue-router'
import { pagesMap } from '@/router'
import UiAlertGroup from './components/uikit/UiAlertGroup.vue'

const { $t } = inject('i18n') as I18N
const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()
const alertsStore = useAlertsStore()

const ERROR_UNAUTHORIZED_ID = 'Unauthorized'
const ERROR_INTERNAL_ID = 'Internal'

const closeAlert = (id: string) => alertsStore.closeAlert(id)

axios.interceptors.response.use(res => res, async err => {
    const originalConfig = err?.config;
    console.log(`ERROR: code '${err?.code}', message: '${err?.message}', url: '${err?.config?.url}'`);
    if (err?.response) {
        switch (err?.response?.status || err?.error?.status) {
            case 400:
                return Promise.resolve()
            case 401:
                if (!originalConfig._retry) {
                    originalConfig._retry = true;
                    try {
                        await authStore.authAccess()
                    } catch (_error: any) {
                        const res = _error as ErrorResponse
                        router.replace({
                            name: pagesMap.login.name,
                            query: { next: route.path }
                        })

                        if (!alertsStore.items.find(item => item.id === ERROR_UNAUTHORIZED_ID)) {
                            alertsStore.createAlert({
                                id: ERROR_UNAUTHORIZED_ID,
                                type: 'danger',
                                text: res.message ?? $t('errors.unauthorized')
                            })
                        }
                        return Promise.resolve()
                    }
                }
                return Promise.reject(err)
            case 500:
            case 503:
                if (!alertsStore.items.find(item => item.id === ERROR_INTERNAL_ID)) {
                    const res = err.response as ErrorResponse

                    alertsStore.createAlert({
                        id: ERROR_INTERNAL_ID,
                        type: 'danger',
                        text: res.message ?? $t('errors.internal')
                    })
                }
                break
        }
    }
    return Promise.resolve();
})
</script>

<style lang="scss">
@mixin styled-scroll {
    scrollbar-width: thin;
    scrollbar-color: var(--pf-global--palette--black-150) transparent;

    &::-webkit-scrollbar {
        margin-top: 1rem;
        display: block;
        width: 0.6rem;
        height: 0.6rem;
    }

    &::-webkit-scrollbar-track {
        background-color: var(--pf-global--BackgroundColor--200);
        border-radius: 0.4rem;
    }

    &::-webkit-scrollbar-thumb {
        background-color: #979da3;
        border-radius: 0.4rem;
    }
}

.pf-icon {
    -moz-osx-font-smoothing: grayscale;
    -webkit-font-smoothing: antialiased;
    display: inline-block;
    font-style: normal;
    font-variant: normal;
    text-rendering: auto;
    line-height: 1;
}

#app {
    min-height: 100vh;
}

.pf-c-page {
    background-color: var(--op-base-background);
    min-height: 100vh;

    &__main-section {
        padding: var(--pf-global--spacer--md);
    }

    &__main {
        z-index: initial
    }
}

.pf-c-menu.pf-m-scrollable {
    .pf-c-menu__content {
        @include styled-scroll();
    }
}

.op-opacity-0 {
    opacity: 0;
}

.app-toast-alerts {
    position: fixed;
    top: 30px;
    right: 30px;
    z-index: 1000;
}
</style>
