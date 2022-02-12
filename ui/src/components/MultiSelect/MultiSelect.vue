<template>
    <VDropdown
        placement="bottom-start"
        class="multi-select"
    >
        <span class="multi-select__action">
            <slot />
        </span>
        <template #popper>
            <div
                class="pf-c-card pf-m-compact pf-u-min-width"
            >
                <MultiSelectList
                    :items="itemsSelect"
                    :selected="selected"
                    @select="select"
                    @deselect="deselect"
                    @search="search"
                />
            </div>
        </template>
    </VDropdown>
</template>

<script setup lang="ts">
import MultiSelectList from "@/components/MultiSelect/MultiSelectList.vue";
import { ref, computed } from "vue";

export interface Item {
    item: any;
    name: string;
}

const emit = defineEmits<{
    (e: "select", item: any): void;
    (e: "deselect", item: any): void;
    (e: "search", payload: string): void;
}>();

const props = defineProps<{
    items: Item[];
    selected?: any;
}>();

let key = ref(0);
const searchRef = ref("");

const itemsSelect = computed(() => {
    if (searchRef.value) {
        return props.items.filter((item: any) => {
            const name = item.name.toLowerCase();

            return name.search(searchRef.value) >= 0;
        });
    } else {
        return props.items;
    }
});

const select = (item: any): void => {
    emit("select", item);
};

const deselect = (item: any): void => {
    emit("deselect", item);
};

const search = (payload: string) => {
    searchRef.value = payload.toLowerCase();
    emit("search", payload);
};
</script>

<style scoped>
.event-select__description {
    border-left: 1px solid #d2d2d2;
}
</style>
