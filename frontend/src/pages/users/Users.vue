<template>
    <section class="pf-c-page__main-section">
        <ToolsLayout>
            <template #title>
                {{ $t('users.title') }}
            </template>
            <template #main>
                <UiCardContainer class="pf-u-h-100">
                    <UiTable
                        :items="items"
                        :columns="columns"
                        :show-select-columns="true"
                        @on-action="onAction"
                    />
                    {{ groupStore.users }}
                </UiCardContainer>
            </template>
        </ToolsLayout>
    </section>
</template>

<script setup lang="ts">
import { ref, computed, inject, onMounted } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { useGroupStore } from '@/stores/group/group';
import { Row, Action } from '@/components/uikit/UiTable/UiTable'
import { GroupRecord } from '@/api';

import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import ToolsLayout from '@/layout/tools/ToolsLayout.vue'
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue'

const i18n = inject<any>('i18n')
const groupStore = useGroupStore();

const items = computed(() => {
    return [];
});

const columns = computed(() => {
    return [];
});

const onAction = (payload: Action) => {
    // TODO
};

onMounted(() => {
    groupStore.getList();
});
</script>

<style scoped lang="scss"></style>
