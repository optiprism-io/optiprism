<template>
    <Select
        :items="items"
        :selected="selectedItem"
        :width-auto="true"
        @select="select"
    >
        <slot />
    </Select>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { findOperations, OperationId } from "@/types";
import { PropertyRef, PropertyType } from "@/types/events"
import { Item } from "@/components/Select/SelectTypes";
import { useLexiconStore } from "@/stores/lexicon";
import Select from "@/components/Select/Select.vue";

const lexiconStore = useLexiconStore();

const props = defineProps<{
    propertyRef: PropertyRef;
    selected?: OperationId;
}>();

const emit = defineEmits<{
    (e: "select", opId: OperationId): void;
}>();

const items = computed(() => {
    let ret: Item<OperationId, null>[] = [];

    if (props.propertyRef.type === PropertyType.Event) {
        const prop = lexiconStore.findEventPropertyById(props.propertyRef.id);
        findOperations(prop.type, prop.nullable, prop.isArray).forEach(op =>
            ret.push({
                item: op.id,
                name: op.name
            })
        );
    } else if (props.propertyRef.type === PropertyType.EventCustom) {
        const prop = lexiconStore.findEventCustomPropertyById(props.propertyRef.id);
        findOperations(prop.type, prop.nullable, prop.isArray).forEach(op =>
            ret.push({
                item: op.id,
                name: op.name
            })
        );
    } else if (props.propertyRef.type === PropertyType.User) {
        const prop = lexiconStore.findUserPropertyById(props.propertyRef.id);
        findOperations(prop.type, prop.nullable, prop.isArray).forEach(op =>
            ret.push({
                item: op.id,
                name: op.name
            })
        );
    } else if (props.propertyRef.type === PropertyType.UserCustom) {
        const prop = lexiconStore.findUserCustomPropertyById(props.propertyRef.id);
        findOperations(prop.type, prop.nullable, prop.isArray).forEach(op =>
            ret.push({
                item: op.id,
                name: op.name
            })
        );
    }

    return ret;
});

let selectedItem = computed((): OperationId | undefined => {
    if (props.selected) {
        return props.selected;
    } else {
        return items?.value[0]?.item;
    }
});

const select = (opId: OperationId) => {
    emit("select", opId);
};
</script>
