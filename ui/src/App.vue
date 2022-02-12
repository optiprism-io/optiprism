<template>
    <div class="pf-c-page">
        <Header />
        <main class="pf-c-page__main">
            <router-view />
        </main>
    </div>
</template>

<script setup lang="ts">
import Header from "@/components/Header.vue";
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
