<template>
    <MultiSelect
        :items="items"
        :selected="selected"
        @select="add"
        @deselect="remove"
    >
        <slot />
    </MultiSelect>
</template>

<script setup lang="ts">
import { Value } from "@/types";
import { PropertyRef } from "@/types/events";
import MultiSelect, { Item } from "@/components/MultiSelect/MultiSelect.vue";

withDefaults(
    defineProps<{
        propertyRef: PropertyRef;
        selected?: Value[];
        items?: Item[];
    }>(),
    {
        selected: () => [],
        items: () => []
    }
);

const emit = defineEmits<{
    (e: "add", value: Value): void;
    (e: "remove", value: Value): void;
}>();

const add = (value: Value) => {
    emit("add", value);
};

const remove = (value: Value) => {
    emit("remove", value);
};
</script>
