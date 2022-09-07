<template>
    <router-view />
</template>

<script lang="ts" setup>
import axios from 'axios'
import { useAuthStore } from '@/stores/auth/auth'

const authStore = useAuthStore()

axios.interceptors.response.use(res => res, async err => {
    const originalConfig = err.config;
    if (err.response) {
        if (err.response.status === 400 && err.response.data) {
            return Promise.reject(err.response.data);
        }

        if (err.response.status === 401 && !originalConfig._retry) {
            /* To prevent infinite loop */
            originalConfig._retry = true;

            try {
                // TODO
            } catch (_error) {
                // TODO
            }
        }
        if (err.response.status === 403 && err.response.data) {
            return Promise.reject(err.response.data);
        }
    }
    return Promise.reject(err);
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
</style>
