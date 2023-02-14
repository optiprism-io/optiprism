<template>
    <header class="app-header">
        <div class="pf-l-flex pf-u-align-items-center">
            <div class="pf-l-flex__item pf-u-ml-md">
                <router-link
                    class="app-header__logo"
                    to="/dashboards"
                    aria-current="page"
                >
                    <img
                        class="pf-c-brand"
                        src="@/assets/img/logo-black.svg"
                        alt="Optyprism"
                    >
                </router-link>
            </div>
            <div class="pf-l-flex__item">
                <Nav />
            </div>
            <div class="pf-l-flex__item pf-m-align-right">
                <div class="app-header__tools">
                    <div class="pf-c-page__header-tools-group">
                        <div class="pf-c-page__header-tools-item">
                            <UiDropdown
                                class="pf-u-mr-md"
                                :items="userMenu"
                                :text-button="'User'"
                                :transparent="true"
                                @select-value="selectUserMenu"
                            />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </header>
</template>

<script setup lang="ts">
import { inject } from 'vue'
import { GenericUiDropdown, UiDropdownItem } from '@/components/uikit/UiDropdown.vue'
import Nav from '@/components/common/Nav.vue'
import { useAuthStore } from '@/stores/auth/auth'
import { useDashboardsStore } from '@/stores/dashboards'
import { useRouter } from 'vue-router'

const authStore = useAuthStore()
const dashboardsStore = useDashboardsStore()
const router = useRouter()
const i18n = inject<any>('i18n')
const UiDropdown = GenericUiDropdown<string>()

const userMenuMap = {
    LOGOUT: 'logout'
}

const userMenu: UiDropdownItem<string>[] = [
    {
        key: 1,
        value: userMenuMap.LOGOUT,
        nameDisplay: i18n.$t(`userMenu.${userMenuMap.LOGOUT}`)
    }
]

const selectUserMenu = (item: UiDropdownItem<string>) => {
    if (item.value === userMenuMap.LOGOUT) {
        authStore.reset()
        authStore.$reset()
        dashboardsStore.$reset()
        router.replace({ name: 'login' })
    }
}
</script>

<style scoped lang="scss">
.app-header {
    position: sticky;
    top: 0;
    z-index: var(--pf-global--ZIndex--2xl);
    height: 44px;
    grid-area: header;
    background-color: var(--op-base-color);

    &__tools {
        margin-left: auto;
        display: flex;
        color: #fff;
    }
    &__logo {
        display: inline-block;
        width: 110px;
        margin-top: 6px;
    }
}
</style>
