<template>
    <h1 class="pf-u-font-size-2xl pf-u-mb-md">
        {{ $t('events.events') }}
    </h1>
    <div class="pf-l-grid pf-m-gutter">
        <div class="pf-l-grid__item">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__body">
                    <UiTable
                        :items="items"
                        :columns="columns"
                    />
                </div>
            </div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed, inject } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import { Event } from '@/types/events'

const i18n = inject<any>('i18n')
const lexiconStore = useLexiconStore()

const columns = computed(() => {
    return ['name', 'displayName', 'description'].map(key => {
        return {
            value: key,
            title: i18n.$t(`events.event_management.columns.${key}`),
        }
    })
})

const items = computed(() => {
    return lexiconStore.events.map((event: Event) => {
        return [
            {
                value: 'name',
                title: event.name
            },
            {
                value: 'displayName',
                title: event.displayName || '',
            },
            {
                value: 'description',
                title: event.description || '',
            }
        ]
    })
})
</script>

<style scoped lang="scss">
</style>
