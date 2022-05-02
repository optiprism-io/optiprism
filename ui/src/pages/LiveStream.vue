<template>
    <EventNav />
    <h1 class="pf-u-font-size-2xl pf-u-mb-md">
        {{ $t('events.live_stream.title') }}
    </h1>
    <div class="pf-l-grid pf-m-gutter">
        <div class="pf-l-grid__item">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>{{ $t('events.events') }}</p>
                </div>
                <div class="pf-c-card__body" />
            </div>
        </div>
        <div class="pf-l-grid__item">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__body" />
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import EventNav from '@/components/events/EventNav.vue'
</script>

<style scoped lang="scss">
.page-title {
    color: var(--op-base-color);
    font-size: 1.4rem;
    margin-bottom: .2rem;
}
</style>
