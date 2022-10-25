<template>
    <ToolsLayout :col-lg="12">
        <template #title>
            {{ $t('events.live_stream.title') }}
        </template>
        <UiCard
            class="pf-c-card pf-m-compact pf-u-h-100"
            :title="$t('events.events')"
        >
            <InputsEventsLiveStream />
        </UiCard>
        <template #main>
            <UiCardContainer class="pf-u-h-100">
                <TableLiveStream />
            </UiCardContainer>
        </template>
    </ToolsLayout>
</template>

<script lang="ts" setup>
import InputsEventsLiveStream from '@/components/events/LiveStream/InputsEventsLiveStream.vue'
import TableLiveStream from '@/components/events/LiveStream/TableLiveStream.vue'
import ToolsLayout from '@/layout/tools/ToolsLayout.vue'
import UiCard from '@/components/uikit/UiCard/UiCard.vue'
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue'
</script>

<style scoped lang="scss">
</style>
