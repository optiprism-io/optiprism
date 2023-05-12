<template>
    <div
        class="properties-management-line pf-u-display-flex pf-m-align-self-baseline pf-u-w-100"
        :class="{
            'properties-management-line_no-edit': props.noEdit,
        }"
    >
        <div
            v-if="noEdit"
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
            v-else
            class="properties-management-line__inputs pf-u-display-flex pf-u-w-100"
        >
            <div
                class="properties-management-line__input pf-u-w-100 pf-u-mr-md"
                @click="onClickInput"
            >
                <UiForm>
                    <UiFormGroup
                        :error="props.errorKey ? $t('common.fillField') : ''"
                        :indent="false"
                        :for="KEY"
                    >
                        <UiInput
                            v-model="editKey"
                            class="pf-u-px-lg pf-u-py-md"
                            :required="true"
                            :name="KEY"
                            :invalid="props.errorKey"
                            :mount-focus="editClickInputType === KEY"
                            autocomplete="new-password"
                            @blur="onEdit"
                        />
                    </UiFormGroup>
                </UiForm>
            </div>
            <div
                class="properties-management-line__input pf-u-w-100 pf-u-mr-md"
                @click="onClickInput"
            >
                <UiInput
                    v-model="editValue"
                    class="pf-u-px-lg pf-u-py-md"
                    :required="true"
                    :name="VALUE"
                    :mount-focus="editClickInputType === VALUE"
                    autocomplete="new-password"
                    @blur="onEdit"
                />
            </div>
        </div>
        <div
            v-show="!props.hideControls"
            class="properties-management-line__controls"
            :class="{
                'properties-management-line__controls_hide': props.hideControls,
            }"
        >
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
import { ref, onBeforeMount } from 'vue';
import { Value } from '@/api';
import UiInput from '@/components/uikit/UiInput.vue';
import UiFormGroup from '@/components/uikit/UiFormGroup.vue';
import UiForm from '@/components/uikit/UiForm.vue';

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
    errorKey?: boolean
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

const editKey = ref('');
const editValue = ref('');
const editMode = ref(false);
const editClickInputType = ref('');
const activeInput = ref(false);

const openEditInputs = (typeInput: string) => {
    editClickInputType.value = typeInput;
    editMode.value = true;
}

const onClickInput = () => {
    activeInput.value = true;
}

const onEdit = () => {
    emit('apply', {
        valueKey: editKey.value,
        value: editValue.value,
        index: props.index,
    });
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