<template>
    <h1 class="pf-u-font-size-2xl pf-u-mb-md">
        {{ $t('events.live_stream.title') }}
    </h1>
    <div class="pf-l-grid pf-m-gutter">
        <div class="pf-l-grid__item">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>{{ $t('events.events') }}</p>
                </div>
                <div class="pf-c-card__body">
                    <InputsEventsLiveStream />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <TableLiveStream />
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { onMounted } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { useLiveStreamStore } from '@/stores/reports/liveStream'

import InputsEventsLiveStream from '@/components/events/LiveStream/InputsEventsLiveStream.vue'
import TableLiveStream from '@/components/events/LiveStream/TableLiveStream.vue'

const lexiconStore = useLexiconStore()
const liveStreamStore = useLiveStreamStore()

onMounted(async () => {
    liveStreamStore.getReportLiveStream()
    await lexiconStore.getEvents()
    await lexiconStore.getEventProperties()
    await lexiconStore.getUserProperties()
})
</script>

<style scoped lang="scss">
</style>
