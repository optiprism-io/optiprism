<template>
    <div class="template-report">
        <div class="pf-u-display-flex pf-u-justify-content-space-between pf-u-align-items-center">
            <div class="template-report__name">
                Untitled Report // TODO
            </div>
            <UiSwitch
                class="pf-u-ml-auto"
                :value="commonStore.syncReports"
                :label="$t('reports.sync')"
                @input="(value: boolean) => commonStore.syncReports = value"
            />
        </div>
        <div class="template-report__content pf-u-mb-md">
            <slot name="content" />
        </div>
        <div class="template-report__views">
            <slot name="views" />
        </div>
    </div>
</template>

<script lang="ts" setup>
import { useCommonStore } from '@/stores/common'

const commonStore = useCommonStore()
</script>