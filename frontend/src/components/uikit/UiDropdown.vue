<template>
    <div class="pf-c-dropdown pf-m-expanded">
        <VDropdown
            :placement="placementMenu"
            :triggers="[]"
            :shown="isOpen"
            @hide="onHide"
        >
            <template v-if="$slots.button">
                <div @click="onToggle">
                    <slot name="button" />
                </div>
            </template>
            <button
                v-else
                class="pf-c-dropdown__toggle"
                :class="{
                    'pf-m-action': isOpen,
                    'pf-m-transparent': transparent && !isOpen,
                    'pf-m-transparent_expanded': transparent && isOpen,
                }"
                aria-expanded="true"
                type="button"
                @click="onToggle"
            >
                <span
                    v-if="textValue"
                    class="pf-c-dropdown__toggle-text"
                    :class="{
                        'pf-u-color-400': !textValue,
                    }"
                >
                    {{ textValue }}
                </span>
                <span
                    v-if="hasIconArrowButton"
                    class="pf-c-dropdown__toggle-icon"
                >
                    <i
                        class="fas fa-caret-down"
                        aria-hidden="true"
                    />
                </span>
            </button>
            <template #popper>
                <div class="pf-c-dropdown">
                    <ul
                        class="pf-c-dropdown__menu"
                        aria-labelledby="dropdown-expanded-button"
                    >
                        <li
                            v-for="item in items"
                            :key="item.key"
                            v-close-popper
                            @click="onClick(item)"
                        >
                            <a
                                v-if="item.href"
                                class="pf-c-dropdown__menu-item"
                                href="#"
                            >
                                {{ item.nameDisplay }}
                            </a>
                            <button
                                v-else
                                class="pf-c-dropdown__menu-item"
                                :class="{
                                    'pf-u-background-color-200': item.selected,
                                }"
                                type="button"
                            >
                                {{ item.nameDisplay }}
                            </button>
                        </li>
                    </ul>
                </div>
            </template>
        </VDropdown>
    </div>
</template>

<script lang="ts">
import { defineComponent, PropType, ref, computed } from 'vue';

export interface UiDropdownItem<T> {
    key: string | number;
    nameDisplay: string;
    value: T;
    selected?: boolean;
    disabled?: boolean;
    iconBefore?: boolean;
    iconAfter?: string;
    href?: string;
    typeButton?: string
    transparent?: boolean
}

class UiDropdownFactory<T = unknown> {
    define() {
        return defineComponent({
            name: 'UiDropdown',
            props: {
                items: {
                    type: Array as PropType<UiDropdownItem<T>[]>,
                    required: true,
                },
                placeholder: {
                    type: String as PropType<string>,
                    default: '',
                },
                hasIconArrowButton: {
                    type: Boolean as PropType<boolean>,
                    default: true,
                },
                textButton: {
                    type: String as PropType<string>,
                    default: '',
                },
                typeButton: {
                    type: String as PropType<string>,
                    default: '',
                },
                placementMenu: {
                    type: String as PropType<string>,
                    default: 'bottom-start',
                },
                isCompact: Boolean as PropType<boolean>,
                transparent: Boolean as PropType<boolean>,
            },
            emits: {
                deselectValue: (payload: UiDropdownItem<T>) => payload,
                selectValue: (payload: UiDropdownItem<T>) => payload
            },
            setup(props, { emit }) {

                const isOpen = ref(false);
                const textValue = computed(() => {
                    return props.textButton ? props.textButton : props.placeholder;
                })

                const onClick = (item: UiDropdownItem<T>) => {
                    emit('selectValue', item);
                };

                const onToggle = () => {
                    isOpen.value = !isOpen.value;
                };

                const onHide = () => {
                    isOpen.value = false;
                };

                return {
                    isOpen,
                    textValue,
                    onClick,
                    onHide,
                    onToggle,
                };
            }
        })
    }
}

const main = new UiDropdownFactory().define();

export function GenericUiDropdown<T>() {
    return main as ReturnType<UiDropdownFactory<T>['define']>;
}

export default main;
</script>

<style lang="scss">
.v-popper {
    &--theme-dropdown {
        .v-popper__arrow-container {
            display: none;
        }
        .v-popper {
            &__inner {
                padding: 0;
                border-radius: 0;
                border: initial;
            }
        }
    }
}

.pf-c-dropdown {
    &__toggle {
        min-width: var(--min-width);
    }

    &__menu {
        position: initial;
        min-width: var(--min-width);
    }

    .pf-m-transparent {
        color: var(--pf-global--Color--light-100);

        &::before {
            content: none;
        }

        &:hover {
            border-bottom: 1px solid #fff;
        }

        &_expanded {
            color: var(--pf-global--Color--light-100);

            &::before {
                border-left: none;
                border-right: none;
                border-top: none;
                border-bottom: 2px solid #fff;
            }
        }
    }
}
</style>
