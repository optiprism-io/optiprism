<template>
    <UiPopupWindow
        :title="title"
        :apply-loading="props.loading"
        class="properties-panagement-popup"
        :apply-button="$t('common.ok')"
        @apply="apply"
        @cancel="close"
    >
        <div class="properties-panagement-popup__content">
            <UiTabs
                class="pf-u-mb-md"
                :items="itemsTabs"
                @on-select="onSelectTab"
            />
            <UiTable
                :compact="true"
                :items="itemsProperties"
                :columns="columnsProperties"
                @on-action="onActionProperty"
            />
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue';
import { Action, Row } from '@/components/uikit/UiTable/UiTable';
import { Value } from '@/api';
import { I18N } from '@/utils/i18n';

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue';
import UiTable from '@/components/uikit/UiTable/UiTable.vue';
import UiCellToolMenu from '@/components/uikit/cells/UiCellToolMenu.vue';
import { useGroupStore } from '@/stores/group/group';
import { GroupRecord } from '@/api';

export type Properties = {
    [key: string]: Value,
};

const i18n = inject('i18n') as I18N;
const groupStore = useGroupStore();
const mapTabs = ['userProperties'];

type Props = {
    item: GroupRecord | null
    loading?: boolean
};

const props = defineProps<Props>();

const emit = defineEmits<{
    (e: 'apply'): void
}>();

const activeTab = ref('userProperties')
const loadingChangeProperties = ref(false);

const title = computed(() => `${i18n.$t('users.user')}: ${props.item?.id}`);

const itemsProperties = computed(() => {
    return props?.item?.properties ? Object.keys(props.item.properties).map((key, i) => {
        return [
            {
                key: 'key',
                title: key,
                nowrap: true,
            },
            {
                key: 'value',
                title: props?.item?.properties[key] || '',
                nowrap: true,
            },
            // TODO IconColumnEdit
        ]
    }) : [];
});

const columnsProperties = computed(() => {
    return [
        {
            value: 'key',
            title: i18n.$t('users.columns.key'),
        },
        {
            value: 'value',
            title: i18n.$t('users.columns.value'),
        },
        {
            value: 'action',
            title: i18n.$t('groups.columns.action'),
            default: true,
            type: 'action',
        },
    ];
});

const itemsTabs = computed(() => {
    return mapTabs.map(key => {
        return {
            name: i18n.$t(`events.event_management.popup.tabs.${key}`),
            active: activeTab.value === key,
            value: key,
        }
    })
});

const onActionProperty = () => {
    // TODO edit property in inputCompnentsCell
};

const onSelectTab = () => {
    // TODO
};

const close = () => {
    apply();
};

const apply = () => {
    emit('apply');
    groupStore.propertyPopup = false;
};
</script>