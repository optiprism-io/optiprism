<template>
    <li class="pf-c-menu__list-item">
        <a
            class="pf-c-menu__item"
            @click="onClick(item)"
        >
            <span class="pf-c-menu__item-main">
                <span class="pf-c-menu__item-icon">
                    <input
                        :checked="selected"
                        class="pf-c-check__input multi-select-list-item__input"
                        type="checkbox"
                    >
                </span>
                <span class="pf-c-menu__item-text">{{ text }}</span>
            </span>
        </a>
    </li>
</template>

<script lang="ts">
import { defineComponent, PropType } from 'vue';

class MultiSelectListItemFactory<T = any> {
    define() {
        return defineComponent({
            name: 'MultiSelectListItem',
            props: {
                item: {
                    type: null as unknown as PropType<T | undefined>,
                    default: undefined as unknown,
                    required: true,
                },
                selected: Boolean,
                text: {
                    type: String,
                    default: '',
                },
            },
            emits: {
                deselect: (payload: T) => payload,
                select: (payload: T) => payload
            },
            setup(props, { emit }) {

                const onClick = (value: T) => {
                    if (props.selected) {
                        emit('deselect', value);
                    } else {
                        emit('select', value);
                    }
                };

                return {
                    onClick,
                };
            }
        })
    }
}

const main = new MultiSelectListItemFactory().define();

export function GenericMultiSelectListItem<T>() {
    return main as ReturnType<MultiSelectListItemFactory<T>['define']>;
}

export default main;
</script>

<style lang="scss" scoped>
.multi-select-list-item {
    &__input {
        pointer-events: none;
    }
}
</style>