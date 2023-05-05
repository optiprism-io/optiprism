<template>
    <div
        class="properties-management-line pf-u-display-flex pf-u-w-100"
        :class="{
            'properties-management-line_no-edit': props.noEdit,
        }"
    >
        <div
            v-if="editMode && !props.noEdit"
            class="properties-management-line__inputs pf-u-display-flex pf-u-w-100"
        >
            <div
                class="properties-management-line__input pf-u-w-100"
                @click="onClickInput"
            >
                <UiInput
                    v-model="editKey"
                    class="pf-u-px-lg pf-u-py-md"
                    :required="true"
                    :name="KEY"
                    :mount-focus="editClickInputType === KEY"
                    @blur="onEdit"
                />
            </div>
            <div
                class="properties-management-line__input pf-u-w-100"
                @click="onClickInput"
            >
                <UiInput
                    v-model="editValue"
                    class="pf-u-px-lg pf-u-py-md"
                    :required="true"
                    :name="VALUE"
                    :mount-focus="editClickInputType === VALUE"
                    @blur="onEdit"
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
                @click="openEditInputs(KEY)"
            >
                {{ props.valueKey }}
            </div>
            <div
                class="properties-management-line__item pf-u-align-items-center pf-u-display-flex pf-u-w-100 pf-u-px-lg"
                :class="{
                    'pf-u-font-weight-bold': props.boldText,
                }"
                @click="openEditInputs(VALUE)"
            >
                {{ props.value }}
            </div>
        </div>
        <div
            v-show="!props.hideControls"
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
    noEdit?: boolean
};

const VALUE = 'value';
const KEY = 'key';

const props = withDefaults(defineProps<Props>(), {
    boldText: false,
    hideControls: false,
    startEdit: false,
    index: -1,
});
const emit = defineEmits<{
    (e: 'apply', payload: ApplyPayload): void
    (e: 'delete', index: number): void
    (e: 'close-new-line'): void
}>();

const i18n = inject('i18n') as I18N;
const editKey = ref('');
const editValue = ref('');
const editMode = ref(false);
const error = ref('');
const editClickInputType = ref('');
const activeInput = ref(false);

const editIcon = computed(() => editMode.value ? 'fas fa-plus-circle' : 'fas fa-pencil-alt');
const editTooltip = computed(() => editMode.value ? i18n.$t('users.saveProperty') : i18n.$t('users.editProperty'));
const isNewLine = computed(() => props.index === -1);
const isHasEditKey = computed(() => Boolean(editKey.value.trim()));
const isHasEditValue = computed(() => Boolean(editValue.value.trim()));

const openEditInputs = (typeInput: string) => {
    editClickInputType.value = typeInput;
    editMode.value = true;
}

const onClickInput = () => {
    activeInput.value = true;
}

const onEdit = () => {
    activeInput.value = false;
    if (editMode.value) {
        setTimeout(() => {
            if (!(activeInput.value && isNewLine.value && (isHasEditKey.value || isHasEditValue.value))) {
                if (editKey.value && (editKey.value !== props.valueKey || editValue.value !== props.value)) {
                    emit('apply', {
                        valueKey: editKey.value,
                        value: editValue.value,
                        index: props.index,
                    });
                } else {
                    if (!editValue.value && !isNewLine.value) {
                        emit('close-new-line');
                    }
                }
                if (editKey.value && !activeInput.value) {
                    editMode.value = false;
                }
            }
        }, 300);
    } else {
        openEditInputs(KEY);
    }
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
    &:hover {
        cursor: pointer;
        .properties-management-line__controls {
            opacity: 1;
        }
    }
    &_no-edit {
        cursor: initial;
        pointer-events: none;
        .properties-management-line__controls {
            opacity: 0;
        }
    }
    &__table {
        margin-left: 80px;
    }
    &__controls {
        opacity: 0;
        &_hide {
            cursor: initial;
            pointer-events: none;
            opacity: 0;
        }
    }
    &__item {
        max-height: 40px;
    }
}
</style>