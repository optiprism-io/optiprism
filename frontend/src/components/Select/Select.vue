<template>
    <VDropdown
        class="select"
        placement="bottom-start"
        :popper-class="props.popperClass || ''"
        :shown="isOpen"
        :container="props.popperContainer || 'body'"
        :auto-hide="props.autoHide"
        @hide="onHide"
    >
        <slot />
        <template #popper="{ hide }">
            <div class="pf-c-card pf-m-display-lg pf-u-min-width">
                <div
                    v-if="loading"
                    class="select__loader-wrap"
                >
                    <UiSpinner class="select__loader" />
                </div>
                <div
                    v-else
                    class="select__content"
                >
                    <div
                        class="select__box"
                        :class="{
                            'select__box_width-auto': widthAuto,
                        }"
                    >
                        <SelectList
                            :items="itemsWithSearch"
                            :grouped="grouped"
                            :selected="selectedItem"
                            :show-search="showSearch"
                            :multiple="props.multiple"
                            @select="($event) => {
                                if (!props.multiple) {
                                    hide()
                                }
                                select($event)
                            }"
                            @hover="hover"
                            @on-search="onSearch"
                            @action="
                                ($event) => {
                                    if (props.cloaseAfterAction) {
                                        hide()
                                    }
                                    onAction($event)
                                }
                            "
                            @edit="emit('edit', $event)"
                        />
                    </div>
                    <div
                        v-if="$slots.description"
                        class="select__description pf-u-pt-lg pf-u-p-sm"
                    >
                        <slot name="description" />
                    </div>
                    <div
                        v-else-if="selectedDescription"
                        class="select__description"
                    >
                        <div class="pf-c-card__body pf-u-pt-lg pf-u-p-sm pf-u-color-200">
                            <div class="select__description-icon">
                                <UiIcon icon="fas fa-info-circle" />
                            </div>
                            <div class="select__description-text">
                                {{ selectedDescription }}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </template>
    </VDropdown>
</template>

<script setup lang="ts">
import { computed, ref, onBeforeMount, watchEffect } from 'vue';
import SelectList from '@/components/Select/SelectList.vue';
import UiSpinner from '@/components/uikit/UiSpinner.vue';
import { Group, Item } from '@/components/Select/SelectTypes';

const emit = defineEmits<{
    (e: 'select', item: any): void;
    (e: 'onSearch', payload: string): void;
    (e: 'onHover', item: any): void;
    (e: 'action', payload: string): void
    (e: 'edit', payload: number): void
}>();

const props = withDefaults(
    defineProps<{
        items: any;
        grouped?: boolean;
        selected?: any;
        loading?: boolean;
        isOpenMount?: boolean;
        updateOpen?: boolean;
        showSearch?: boolean;
        widthAuto?: boolean;
        popperClass?: string
        popperContainer?: string
        autoHide?: boolean
        multiple?: boolean
        cloaseAfterAction?: boolean
    }>(),
    {
        showSearch: true,
        grouped: false,
        selected: false,
        isOpenMount: false,
        updateOpen: false,
        popperClass: undefined,
        autoHide: true,
        popperContainer: 'body',
        multiple: false,
        cloaseAfterAction: false,
    }
);

const key = ref(0);
const isOpen = ref(false);
const selectedItemLocal = ref(false);
const search = ref('');
const description = ref();

const firstElement = computed(() => props.items[0]);

const selectedItem = computed(() => {
    if (props.grouped) {
        return selectedItemLocal.value || props.selected || firstElement.value?.items[0]?.item;
    } else {
        return selectedItemLocal.value || props.selected || firstElement.value?.item;
    }
});

const itemsWithSearch = computed(() => {
    if (search.value) {
        if (props.grouped) {
            return props.items.reduce((acc: Group<any>[], item: Group<any>) => {
                const innerItems: Item<any, any>[] = item.items.filter((item: Item<any, any>) => {
                    const name = item.name.toLowerCase();

                    return name.search(search.value) >= 0;
                });

                if (innerItems.length) {
                    acc.push({
                        ...item,
                        items: innerItems
                    });
                }

                return acc;
            }, []);
        } else {
            return props.items.filter((item: any) => {
                const name = item.name.toLowerCase();

                return name.search(search.value) >= 0;
            });
        }
    } else {
        return props.items;
    }
});

const selectedDescription = computed(() => {
    let item: any = null;
    if (props.grouped) {
        itemsWithSearch.value.forEach((group: Group<any>) => {
            group.items.forEach((groupItem: Item<any, any>) => {
                if (JSON.stringify(selectedItem.value) === JSON.stringify(groupItem.item)) {
                    item = groupItem;
                }
            });
        });
    } else {
        itemsWithSearch.value.forEach((groupItem: Item<any, any>) => {
            if (JSON.stringify(selectedItem.value) === JSON.stringify(groupItem.item)) {
                item = groupItem;
            }
        });
    }

    return item ? item.description : '';
});

watchEffect(() => {
    if (props.updateOpen) {
        isOpen.value = true;
    }
})

const select = (item: any): void => {
    isOpen.value = false;
    selectedItemLocal.value = false;
    key.value++;
    emit('select', item);
};

const hover = (item: Item<any, any>): void => {
    if (item) {
        description.value = item?.description || '';
        selectedItemLocal.value = item.item;
        emit('onHover', item.item);
    }
};

const onSearch = (payload: string) => {
    search.value = payload.toLowerCase();
    description.value = '';
    emit('onSearch', payload);
};

const onHide = () => {
    isOpen.value = false;
    search.value = '';
};

const onAction = (payload: string) => {
    emit('action', payload);
}

onBeforeMount(() => {
    isOpen.value = props.isOpenMount;
});
</script>

<style scoped lang="scss">
.select {
    position: relative;

    &__content {
        display: flex;
    }

    &__box {
        width: 230px;
        flex: 1;

        &_width-auto {
            width: initial;
        }
    }

    &__loader-wrap {
        min-width: 20rem;
        min-height: 18rem;
    }

    &__loader {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
    }

    &__description {
        min-width: 200px;
        border-left: 1px solid var(--pf-global--BackgroundColor--200);
    }

    &__description-icon {
        float: left;
        margin-right: .5rem;
    }

    &__description-text {
        font-size: .9rem;
        max-width: 200px;
    }
}
</style>
