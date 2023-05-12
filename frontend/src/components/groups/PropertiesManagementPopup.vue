<template>
    <UiPopupWindow
        :title="title"
        :apply-loading="props.loading"
        class="properties-panagement-popup"
        :apply-button="$t('common.save')"
        :cancel-button="$t('common.close')"
        :apply-disabled="applyDisabled"
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
                class="properties-panagement-popup__line pf-u-mb-md"
                :hide-controls="false"
                :index="i"
                :value="property.value"
                :value-key="property.key"
                :error-key="property.error"
                @apply="onApplyChangePropery"
                @delete="onDeleteLine"
            />
            <UiButton
                class="pf-m-primary pf-u-mt-md"
                @click="onAddProperty"
            >
                {{ $t('common.addPropery') }}
            </UiButton>
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref, onMounted, onUnmounted } from 'vue';
import { I18N } from '@/utils/i18n';
import { Value, GroupRecord } from '@/api';
import { useGroupStore } from '@/stores/group/group';
import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue';
import PropertiesManagementLine, { ApplyPayload } from './PropertiesManagementLine.vue';

export type Properties = {
    [key: string]: Value,
};

type PropertiesEdit = {
    value: Value,
    key: string,
    error?: boolean,
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
const isLodingSavePropetries = ref(false);
const propertiesEdit = ref<PropertiesEdit[]>([]);

const title = computed(() => `${i18n.$t('users.user')}: ${props.item?.id}`);
const itemsTabs = computed(() => {
    return mapTabs.map(key => {
        return {
            name: i18n.$t(`events.event_management.popup.tabs.${key}`),
            active: activeTab.value === key,
            value: key,
        }
    })
});

const itemsProperties = computed(() => {
    return propertiesEdit.value.map((item, i) => {
        return {
            key: item.key,
            value: item.value || '' as Value,
            error: item.error,
            index: i,
        };
    });
});

const properties = computed(() => {
    return itemsProperties.value.reduce((acc: Properties, item) => {
        acc[item.key] = item.value;
        return acc;
    }, {});
});

const applyDisabled = computed(() => {
    return JSON.stringify(properties.value) === JSON.stringify(props.item?.properties);
});

onMounted(() => {
    propertiesEdit.value = props.item?.properties ?
        Object.keys(props.item.properties).map(key => {
            return {
                value: props.item?.properties[key] || '',
                key: key || '',
            }
        }) : [];
});

onUnmounted(() => {
    isLodingSavePropetries.value = false;
});

const onApplyChangePropery = async (payload: ApplyPayload) => {
    if (props.item?.id) {
        propertiesEdit.value[payload.index] = {
            key: payload.valueKey,
            value: payload.value,
            error: !payload.valueKey.trim(),
        };
    }
};


const onAddProperty = () => {
    propertiesEdit.value.push({
        key: '',
        value: '',
    });
};

const onDeleteLine = async (index: number) => {
    if (props.item?.id) {
        propertiesEdit.value.splice(index, 1);
    }
};

const close = () => {
    groupStore.propertyPopup = false;
};

const checkError = () => {
    propertiesEdit.value = propertiesEdit.value.map(item => {
        return {
            ...item,
            error: !item.key.trim(),
        };
    });
};

const apply = async () => {
    if (props.item?.id) {
        const error = propertiesEdit.value.findIndex(item => !item.key.trim());
        if (error === -1) {
            isLodingSavePropetries.value = true;
            await groupStore.update({
                id: props.item.id,
                properties: propertiesEdit.value.reduce((acc: Properties, item) => {
                    acc[item.key] = item.value;
                    return acc;
                }, {}),
                noLoading: true,
            });
            emit('apply');
            groupStore.propertyPopup = false;
        } else {
            checkError();
        }
    }
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
}

</style>