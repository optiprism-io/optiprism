import {computed, DefineComponent, defineComponent, PropType, Ref, ref} from 'vue';
import UiSelectItem from '@/components/uikit/UiSelect/UiSelectItem.vue'
import {UiSelectItemInterface} from './types';
import UiSelectGroup from '@/components/uikit/UiSelect/UiSelectGroup.vue'
import UiIcon from '@/components/uikit/UiIcon.vue'
import '@/components/uikit/UiSelect/styles.scss'

declare const VDropdown: DefineComponent<{ placement: string }>;

export function UiSelectGeneric<T>() {
    return defineComponent({
        name: 'UiSelect',
        components: {
            UiSelectItem,
        },
        props: {
            items: {
                type: Array as PropType<UiSelectItemInterface<T>[]>,
                default: () => []
            },
            modelValue: {
                type: [String, Number, Object] as PropType<T | null>,
                default: null
            },
            showSearch: {
                type: Boolean,
                default: true
            },
        },
        emits: ['update:modelValue', 'search'],
        setup(props, {slots, emit}) {
            const hovered = ref<UiSelectItemInterface<T, 'item'> | null>(null)
            const search = ref('')

            const filteredItems = computed(() => {
                return props.items.filter(item => {
                    if (item.__type === 'item') {
                        return item.label.toLowerCase().includes(search.value.toLowerCase())
                    } else {
                        return !!item.items.find(subItem => {
                            return subItem.label.toLowerCase().includes(search.value.toLowerCase())
                        })
                    }
                })
            })

            const handleInput = (e: Event): void => {
                search.value = (e.target as HTMLInputElement).value;
                emit('search', search.value, filteredItems.value)
            }

            const handleSelect = (item: T, callback?: () => void): void => {
                emit('update:modelValue', item)
                callback?.()
            }

            const handleMouseOver = (item: UiSelectItemInterface<T, 'item'>): void => {
                (hovered as Ref<UiSelectItemInterface<T, 'item'>>).value = item
            }

            const handleMouseOut = (): void => {
                hovered.value = null
            }

            return () => (
                <VDropdown class="ui-select" placement="bottom-start">
                    {{
                        default: () => <div class="relative">{ slots.default?.() }</div>,
                        popper: ({ hide }: { hide: () => void }) => (
                            <div class="pf-c-card pf-m-display-lg pf-u-min-width">
                                <div class="ui-select__content">
                                    <div class="ui-select__box">
                                        <div class="pf-c-menu pf-m-plain pf-m-scrollable">
                                            {
                                                props.showSearch && (
                                                    <div class="pf-c-menu__search">
                                                        <div class="pf-c-menu__search-input">
                                                            <input
                                                                class="pf-c-form-control pf-m-search"
                                                                type="search"
                                                                name="search-input"
                                                                aria-label="Search"
                                                                value={search.value}
                                                                onInput={handleInput}
                                                            />
                                                        </div>
                                                    </div>
                                                )
                                            }
                                            {
                                                filteredItems.value.length > 0 && (
                                                    <div class="pf-c-menu__content">
                                                        <ul class="pf-c-menu__list">
                                                            {
                                                                filteredItems.value.map(item => {
                                                                    if (item.__type === 'item') {
                                                                        return <UiSelectItem
                                                                            key={item.id}
                                                                            label={item.label}
                                                                            selected={item.value === props.modelValue}
                                                                            onClick={() => handleSelect(item.value, hide)}
                                                                            onMouseOver={() => handleMouseOver(item)}
                                                                            onMouseOut={handleMouseOut}
                                                                        />
                                                                    } else {
                                                                        return <UiSelectGroup label={item.label}>
                                                                            {
                                                                                item.items.map(subItem => {
                                                                                    return <UiSelectItem
                                                                                        key={item.id}
                                                                                        label={subItem.label}
                                                                                        onClick={() => handleSelect(subItem.value, hide)}
                                                                                    />
                                                                                })
                                                                            }
                                                                        </UiSelectGroup>
                                                                    }
                                                                })
                                                            }
                                                        </ul>
                                                    </div>
                                                )
                                            }
                                        </div>
                                    </div>
                                    {
                                        slots.description && hovered.value && (
                                            <div class="ui-select__description">
                                                <div class="pf-c-card__body pf-u-pt-lg pf-u-p-sm pf-u-color-200">
                                                    <div class="pf-l-flex">
                                                        <div class="select__description-icon pf-l-flex__item">
                                                            <UiIcon icon="fas fa-info-circle"/>
                                                        </div>
                                                        <div class="select__description-text pf-l-flex__item">
                                                            {slots.description?.({item: hovered})}
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        )
                                    }
                                </div>
                            </div>
                        )
                    }}
                </VDropdown>
            )
        }
    })
}
