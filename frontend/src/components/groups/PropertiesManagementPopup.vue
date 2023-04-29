<template>
    <UiPopupWindow
        :title="title"
        :apply-loading="props.loading"
        class="properties-panagement-popup"
        :apply-button="$t('common.close')"
        @apply="apply"
        @cancel="close"
    >
        <div class="properties-panagement-popup__content">
            <div
                v-show="isLodingSavePropetries"
                class="properties-panagement-popup__loading"
            >
                <UiSpinner :size="'xl'" />
            </div>
            <UiTabs
                class="pf-u-mb-md"
                :items="itemsTabs"
            />
            <PropertiesManagementLine
                class="properties-panagement-popup__line"
                :hide-controls="true"
                :bold-text="true"
                :no-edit="true"
                :value="$t('users.columns.value')"
                :value-key="$t('users.columns.key')"
            />
            <PropertiesManagementLine
                v-for="(property, i) in itemsProperties"
                :key="i"
                class="properties-panagement-popup__line"
                :hide-controls="false"
                :index="i"
                :value="property.value"
                :value-key="property.key"
                @apply="onApplyChangePropery"
                @delete="onDeleteLine"
            />
            <PropertiesManagementLine
                v-if="createNewLine"
                class="properties-panagement-popup__line"
                :bold-text="false"
                :value="''"
                :value-key="''"
                :start-edit="true"
                :index="-1"
                @apply="onApplyChangePropery"
                @delete="onDeleteNewLine"
                @close-new-line="onDeleteNewLine"
            />
            <UiButton
                v-else
                class="pf-m-primary pf-u-mt-md"
                @click="onAddProperty"
            >
                {{ $t('common.addPropery') }}
            </UiButton>
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue';
import { I18N } from '@/utils/i18n';
import { Value, GroupRecord } from '@/api';
import { useGroupStore } from '@/stores/group/group';
import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue';
import PropertiesManagementLine, { ApplyPayload } from './PropertiesManagementLine.vue';

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

const activeTab = ref('userProperties');
const createNewLine = ref(false);
const isLodingSavePropetries = ref(false);

const title = computed(() => `${i18n.$t('users.user')}: ${props.item?.id}`);
const activeItem = computed(() => groupStore.items.find(item => item.id === props.item?.id));
const activeItemProperties = computed(() => activeItem.value?.properties || {});

const itemsProperties = computed(() => {
    return Object.keys(activeItemProperties.value).map((key, i) => {
        return {
            key,
            value: activeItemProperties.value[key] || '' as Value,
            index: i,
        };
    });
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

const onApplyChangePropery = async (payload: ApplyPayload) => {
    if (props.item?.id) {
        isLodingSavePropetries.value = true;
        let properties: Properties = {};
        const activeItemPropertiesLength = Object.keys(activeItemProperties.value).length;

        if (payload.index === -1) {
            properties = {
                ...activeItemProperties.value,
                [payload.valueKey]: payload.value,
            };
        } else {
            const items = [...itemsProperties.value];
            items[payload.index].key = payload.valueKey;
            items[payload.index].value = payload.value;
            items.forEach(item => {
                properties[item.key] = item.value;
            });
        }
        const propertiesLength = Object.keys(properties).length;

        await groupStore.update({
            id: props.item.id,
            properties,
            noLoading: true,
        });
        if (propertiesLength > activeItemPropertiesLength || (!payload.valueKey && !payload.value)) {
            createNewLine.value = false;
        }
        isLodingSavePropetries.value = false;
    }
};

const onAddProperty = () => {
    createNewLine.value = true;
};

const onDeleteNewLine = () => {
    createNewLine.value = false;
};

const onDeleteLine = async (index: number) => {
    if (props.item?.id) {
        isLodingSavePropetries.value = true;
        await groupStore.update({
            id: props.item.id,
            properties: itemsProperties.value.reduce((acc: Properties, item, i) => {
                if (i !== index) {
                    acc[item.key] = item.value;
                }
                return acc;
            }, {}),
            noLoading: true,
        });
        isLodingSavePropetries.value = false;
    }
};

const close = () => {
    apply();
};

const apply = () => {
    emit('apply');
    groupStore.propertyPopup = false;
};
</script>

<style lang="scss">
.properties-panagement-popup {
    .pf-c-table {
        margin-right: 80px;
    }
    &__content {
        position: relative;
    }
    &__loading {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
        background-color: rgba(#fff, .6);
        z-index: 2;
    }
    &__line {
        border-bottom: 1px solid var(--pf-global--BorderColor--dark-100);
    }
}

</style>