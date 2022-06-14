<template>
    <div class="ui-tags">
        <span
            v-for="(item, i) in props.value"
            :key="item"
            class="ui-tags__item pf-u-mr-sm pf-u-mb-sm"
        >
            <UiTag
                :value="item"
                :index="i"
                :editable="props.editable"
                class="pf-m-blue"
                @input="onInput"
            />
            <i
                v-if="props.editable"
                class="ui-tags__item-remove"
                @click="onRemoveItem(i)"
            >
                <UiIcon :icon="'fas fa-times'" />
            </i>
        </span>
        <span
            v-if="props.editable"
            class="ui-tags__item-add"
            :class="{
                'ui-tags__item-add_visible': showNew
            }"
        >
            <UiTag
                v-if="showNew"
                :value="''"
                :index="0"
                :editable="true"
                :mount-focus="true"
                class="pf-m-blue"
                @input="onInputNew"
            />
            <i
                v-else
                @click="addOne"
            >
                <UiIcon :icon="'fas fa-plus'" />
            </i>
        </span>
    </div>
</template>

<script lang="ts" setup>
import { ref } from 'vue'
import UiTag from './UiTag.vue'
import UiIcon from './UiIcon.vue'

interface Props {
    value?: string[]
    editable?: boolean
}

const emit = defineEmits(['input']);

const props = defineProps<Props>()

const showNew = ref(false)

const onInput = (value: string, index?: number) => {
    if (props.value && index !== undefined) {
        const newValue = props.value

        newValue[index] = value
        if (value) {
            emit('input', newValue)
        } else {
            onRemoveItem(index)
        }
    }
}

const onRemoveItem = (index: number) => {
    if (props.value) {
        const newValue = props.value
        newValue.splice(index, 1)
        emit('input', newValue)
    }
}

const addOne = () => {
    showNew.value = true
}

const onInputNew = (value: string) => {
    if (props.value && value) {
        const newValue = props.value
        newValue.push(value)

        emit('input', newValue)
    }

    showNew.value = false
}
</script>

<style lang="scss">
.ui-tags {
    &__item-remove {
        display: none;
        position: absolute;
        top: -6px;
        right: -6px;
        padding: 0 4px;
        font-size: 10px;
        background-color: #fff;
        border-radius: 50%;
        border: 1px solid #000;
        cursor: pointer;
    }

    &__item {
        position: relative;
        display: inline-block;

        &:hover {
            .ui-tags__item-remove {
                display: block;
            }
        }
    }

    &__item-add {
        opacity: 0;
        cursor: pointer;

        &_visible {
            opacity: 1;
        }
    }

    &:hover {
        .ui-tags__item-add {
            opacity: 1;
        }
    }
}
</style>