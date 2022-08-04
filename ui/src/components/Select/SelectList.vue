<template>
    <div class="pf-c-menu pf-m-plain pf-m-scrollable">
        <div
            v-if="showSearch"
            class="pf-c-menu__search"
        >
            <div class="pf-c-menu__search-input">
                <input
                    v-model="search"
                    class="pf-c-form-control pf-m-search"
                    type="search"
                    name="search-input"
                    aria-label="Search"
                    @input="onSearch"
                >
            </div>
        </div>
        <div
            class="pf-c-menu__content"
            :class="{
                'pf-c-select': props.multiple
            }"
        >
            <template v-if="grouped">
                <template
                    v-for="(group, index) in groupedItems"
                    :key="group.name"
                >
                    <template v-if="group.name">
                        <section class="pf-c-menu__group">
                            <hr
                                v-if="index > 0"
                                class="pf-c-divider"
                            >
                            <div class="pf-c-action-list">
                                <div class="pf-c-action-list__item">
                                    <div class="pf-c-menu__group-title">
                                        {{ group.name }}
                                    </div>
                                </div>
                                <div
                                    v-if="group.action"
                                    class="pf-c-action-list__item pf-u-pt-md"
                                >
                                    <UiButton
                                        class="pf-m-link"
                                        :before-icon="group.action.icon"
                                        @click="onAction(group.action ? group.action.type : '')"
                                    >
                                        {{ $t(group.action.text) }}
                                    </UiButton>
                                </div>
                            </div>

                            <ul class="pf-c-menu__list">
                                <SelectListItem
                                    v-for="(item, i) in group.items"
                                    :key="i"
                                    :item="item.item"
                                    :text="item.name"
                                    :selected="selected"
                                    :is-disabled="item.disabled"
                                    :editable="item.editable"
                                    :multiple="props.multiple"
                                    :active="item.selected"
                                    @mouseenter="hover(item)"
                                    @click="select"
                                    @edit="emit('edit', $event)"
                                />
                            </ul>
                        </section>
                    </template>
                    <ul
                        v-else
                        class="pf-c-menu__list"
                    >
                        <SelectListItem
                            v-for="item in group.items"
                            :key="item.item?.id || item.item"
                            :item="item.item"
                            :text="item.name"
                            :selected="selected"
                            :multiple="props.multiple"
                            :active="item.selected"
                            @mouseenter="hover(item)"
                            @click="select"
                        />
                    </ul>
                </template>
            </template>
            <template v-else>
                <ul class="pf-c-menu__list">
                    <SelectListItem
                        v-for="item in itemItems"
                        :key="item.item.id"
                        :item="item.item"
                        :items="item.items || undefined"
                        :text="item.name"
                        :selected="selected"
                        :multiple="props.multiple"
                        :active="item.selected"
                        @mouseenter="hover(item)"
                        @click="select"
                    />
                </ul>
            </template>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import { Group, Item } from '@/components/Select/SelectTypes';
import SelectListItem from '@/components/Select/SelectListItem.vue';

const emit = defineEmits<{
    (e: 'select', item: any): void;
    (e: 'hover', item: any): void;
    (e: 'on-search', value: string): void;
    (e: 'action', payload: string): void
    (e: 'edit', payload: number): void
}>();

const props = defineProps<{
    items: Item<any, any>[] | Group<any>[];
    grouped: boolean;
    selected?: any;
    showSearch?: boolean;
    multiple?: boolean
}>();

const search = ref('');

const groupedItems = computed((): Group<any>[] => {
    if (props.grouped) {
        return props.items as Group<any>[];
    } else {
        return [];
    }
});

const itemItems = computed((): Item<any, any>[] => {
    if (props.grouped) {
        return [];
    } else {
        return props.items as Item<any, any>[];
    }
});

const hover = (item: any): void => {
    emit('hover', item);
};

const select = (item: any): void => {
    emit('select', item);
};

const onSearch = (): void => {
    emit('on-search', search.value);
};

const onAction = (payload: string) => {
    emit('action', payload)
}
</script>
