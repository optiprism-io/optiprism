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
        <div class="pf-c-menu__content">
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
                            <div class="pf-c-menu__group-title">
                                {{ group.name }}
                            </div>
                            <ul class="pf-c-menu__list">
                                <SelectListItem
                                    v-for="(item, i) in group.items"
                                    :key="i"
                                    :item="item.item"
                                    :text="item.name"
                                    :selected="selected"
                                    :is-disabled="item.disabled"
                                    @mouseenter="hover(item)"
                                    @click="select"
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
                            :key="item.item.id"
                            :item="item.item"
                            :text="item.name"
                            :selected="selected"
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
                        @mouseenter="hover(item)"
                        @click="select"
                    />
                </ul>
            </template>
        </div>
    </div>
</template>

<script setup lang="ts">
// TODO add generics
import { computed, ref } from "vue";
import { Group, Item } from "@/components/Select/SelectTypes";
import SelectListItem from "@/components/Select/SelectListItem.vue";

const emit = defineEmits<{
    (e: "select", item: any): void;
    (e: "hover", item: any): void;
    (e: "on-search", value: string): void;
}>();

const props = defineProps<{
    items: Item<any, any>[] | Group<any>[];
    grouped: boolean;
    selected?: any;
    showSearch?: boolean;
}>();

const search = ref("");

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
    emit("hover", item);
};

const select = (item: any): void => {
    emit("select", item);
};

const onSearch = (): void => {
    emit("on-search", search.value);
};
</script>
