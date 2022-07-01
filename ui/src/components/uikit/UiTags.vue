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
                class="pf-m-blue"
            />
        </span>
    </div>
</template>

<script lang="ts" setup>
import UiTag from './UiTag.vue'

interface Props {
    value?: string[]
}

const props = defineProps<Props>()
</script>

<style lang="scss">
.ui-tags {
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