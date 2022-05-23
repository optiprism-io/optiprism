<template>
    <header class="app-header">
        <div class="pf-l-flex">
            <div class="pf-l-flex__item">
                <div class="app-header__menu">
                    <UiDropdown
                        :items="items"
                        :text-button="'Dashboard'"
                        :transparent="true"
                    />
                </div>
            </div>
            <div class="pf-l-flex__item">
                <Nav />
            </div>
            <div class="pf-l-flex__item pf-m-align-right">
                <div class="app-header__tools">
                    <div class="pf-c-page__header-tools-group">
                        <div class="pf-c-page__header-tools-item">
                            <UiButton
                                :icon="'fas fa-cog'"
                                class="pf-m-base-light"
                                aria-label="Settings"
                            />
                        </div>
                        <div class="pf-c-page__header-tools-item">
                            <UiButton
                                :icon="'pf-icon pf-icon-help'"
                                class="pf-m-base-light"
                                aria-label="Help"
                            />
                        </div>
                    </div>
                    <div class="pf-c-page__header-tools-group">
                        <div class="pf-c-page__header-tools-item">
                            <UiDropdown
                                :items="items"
                                :text-button="'User'"
                                :transparent="true"
                            />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </header>
</template>

<script setup lang="ts">
import UiDropdown from '@/components/uikit/UiDropdown.vue'
import Nav from '@/components/common/Nav.vue'

const items = [
    {
        key: 1,
        value: 1,
        nameDisplay: 'Menu Item 1'
    },
    {
        key: 2,
        value: 2,
        nameDisplay: 'Menu Item 2'
    }
];

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
}
</style>
