<template>
    <div class="properties-management-line pf-u-display-flex pf-u-w-100">
        <div
            v-if="editMode"
            class="properties-management-line__inputs pf-u-display-flex pf-u-w-100"
        >
            <div class="properties-management-line__input pf-u-w-100">
                <UiInput
                    v-model="editKey"
                    class="pf-u-px-lg pf-u-py-md"
                    :required="true"
                    :name="'key'"
                />
            </div>
            <div class="properties-management-line__input pf-u-w-100">
                <UiInput
                    v-model="editValue"
                    class="pf-u-px-lg pf-u-py-md"
                    :required="true"
                    :name="'key'"
                />
            </div>
        </div>
        <div
            v-else
            class="properties-management-line__values pf-u-display-flex pf-u-w-100"
        >
            <div
                class="properties-management-line__item pf-u-align-items-center pf-u-display-flex pf-u-w-100 pf-u-px-lg"
                :class="{
                    'pf-u-font-weight-bold': props.boldText,
                }"
            >
                {{ props.valueKey }}
            </div>
            <div
                class="properties-management-line__item pf-u-align-items-center pf-u-display-flex pf-u-w-100 pf-u-px-lg"
                :class="{
                    'pf-u-font-weight-bold': props.boldText,
                }"
            >
                {{ props.value }}
            </div>
        </div>
        <div
            class="properties-management-line__controls pf-u-display-flex"
            :class="{
                'properties-management-line__controls_hide': props.hideControls,
            }"
        >
            <VTooltip popper-class="ui-hint">
                <button
                    class="pf-c-button pf-m-control"
                    type="button"
                    @click="onEdit"
                >
                    <UiIcon
                        :icon="editIcon"
                    />
                </button>
                <template #popper>
                    {{ editTooltip }}
                </template>
            </VTooltip>
            <button
                class="pf-c-button pf-m-control"
                type="button"
                @click="onDelete"
            >
                <UiIcon icon="fas fa-trash" />
            </button>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { ref, computed, inject, onBeforeMount } from 'vue';
import { Value } from '@/api';
import UiInput from '@/components/uikit/UiInput.vue';
import { I18N } from '@/utils/i18n';

export type ApplyPayload = {
    value: Value,
    valueKey: string,
    index: number
};

type Props = {
    value: Value,
    valueKey: string,
    index?: number
    hideControls?: boolean
    boldText?: boolean
    startEdit?: boolean
};

const props = withDefaults(defineProps<Props>(), {
    boldText: false,
    hideControls: false,
    startEdit: false,
    index: 0,
});
const emit = defineEmits<{
    (e: 'apply', payload: ApplyPayload): void
    (e: 'delete', index: number): void
}>();

const i18n = inject('i18n') as I18N;
const editKey = ref('');
const editValue = ref('');
const editMode = ref(false);

const editIcon = computed(() => editMode.value ? 'fas fa-plus-circle' : 'fas fa-pencil-alt');
const editTooltip = computed(() => editMode.value ? i18n.$t('users.saveProperty') : i18n.$t('users.editProperty'));

const onEdit = () => {
    if (editMode.value) {
        emit('apply', {
            valueKey: editKey.value,
            value: editValue.value,
            index: props.index || 0,
        });
    } else {
        editKey.value = props.valueKey;
        editValue.value = String(props.value);
    }
    editMode.value = !editMode.value;
};

const onDelete = () => {
    emit('delete', (props?.index || 0));
};

onBeforeMount(() => {
    editKey.value = props.valueKey;
    editValue.value = String(props.value);
    editMode.value = props.startEdit;
});
</script>

<style lang="scss">
.properties-management-line {
    &__table {
        margin-left: 80px;
    }
    &__controls {
        &_hide {
            pointer-events: none;
            opacity: 0;
        }
    }
    &__item {
        max-height: 40px;
    }
}
</style>