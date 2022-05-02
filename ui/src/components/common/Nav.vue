<template>
    <nav class="pf-c-nav pf-m-horizontal-subnav" aria-label="Local">
        <ul class="pf-c-nav__list">
            <li class="pf-c-nav__item">
                <router-link
                    to="/"
                    class="pf-c-nav__link pf-m-current"
                    aria-current="page"
                >
                    {{ $t('events.events') }}
                </router-link>
            </li>
            <li class="pf-c-nav__item">
                <router-link
                    to="/"
                    class="pf-c-nav__link"
                    aria-current="page"
                >
                    Users
                </router-link>
            </li>
        </ul>
    </nav>
</template>

<script lang="ts" setup>

</script>

<style scoped>
.pf-c-nav__link.pf-m-current,
.pf-c-nav__link.pf-m-current:hover,
.pf-c-nav__item.pf-m-current:not(.pf-m-expanded) .pf-c-nav__link {
    color: var(--pf-c-nav__link--m-current--Color);
    background-color: var(--pf-global--palette--light-blue-700);
}

</style>