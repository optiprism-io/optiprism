<template>
    <Select
        :items="items"
        grouped
        :selected="selectedItem"
        @select="select"
    >
        <slot />
    </Select>
</template>

<script setup lang="ts">
import { computed } from "vue";
import Select from "@/components/Select/Select.vue";
import { Group, Item } from "@/components/Select/SelectTypes";
import {
    Breakdown,
    newBreakdownCohort,
    newBreakdownEventCommonProperty,
    newBreakdownUserCustomProperty,
    newBreakdownUserProperty,
    BreakdownUserProperty,
    BreakdownUserCustomProperty,
    BreakdownCohort,
    BreakdownEventCommonProperty,
} from "@/stores/eventSegmentation/breakdowns";
import { UserCustomProperty, UserProperty } from "@/types/events";
import { useLexiconStore } from "@/stores/lexicon";
import { useEventsStore } from "@/stores/eventSegmentation/events";

const emit = defineEmits<{
    (e: "select", type: Breakdown): void;
}>();

const props = defineProps<{
    selected?: Breakdown;
}>();

const lexiconStore = useLexiconStore();
const eventStore = useEventsStore();
const events = eventStore.events;

const items = computed(() => {
    let ret: Group<Item<BreakdownCohort | BreakdownUserProperty | BreakdownUserCustomProperty | BreakdownEventCommonProperty, null>[]>[] = [];
    {
        let items: Item<BreakdownCohort, null>[] = [];
        items.push({ item: newBreakdownCohort(), name: "Cohort" });
        ret.push({ name: "", items: items });
    }

    if (lexiconStore.userProperties.length > 0) {
        let items: Item<BreakdownUserProperty, null>[] = [];
        lexiconStore.userProperties.forEach((prop: UserProperty): void => {
            items.push({
                item: newBreakdownUserProperty(prop.id),
                name: prop.name
            });
        });
        ret.push({ name: "User Properties", items: items });
    }

    if (lexiconStore.userCustomProperties.length > 0) {
        let items: Item<BreakdownUserCustomProperty, null>[] = [];
        lexiconStore.userCustomProperties.forEach((prop: UserCustomProperty): void => {
            items.push({
                item: newBreakdownUserCustomProperty(prop.id),
                name: prop.name
            });
        });
        ret.push({ name: "User Custom Properties", items: items });
    }

    if (events.length > 0) {
        let firstProps = lexiconStore.findEventProperties(events[0].ref.id);
        let firstCustomProps = lexiconStore.findEventCustomProperties(events[0].ref.id);
        if (firstProps.length > 0) {
            for (let i = 1; i < events.length; i++) {
                let props = lexiconStore.findEventProperties(events[i].ref.id);
                let rem: number[] = [];
                for (let j = 0; j < firstProps.length; j++) {
                    let firstProp = firstProps[j];
                    let found = false;
                    for (let curProp of props) {
                        if (
                            firstProp.name === curProp.name &&
                            firstProp.isArray === curProp.isArray &&
                            firstProp.isDictionary === curProp.isDictionary
                        ) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        rem.push(j);
                    }
                }

                rem.forEach(idx => firstProps.splice(idx, 1));
            }

            if (firstCustomProps.length > 0) {
                for (let i = 1; i < events.length; i++) {
                    let props = lexiconStore.findEventCustomProperties(events[i].ref.id);
                    let rem: number[] = [];
                    for (let j = 0; j < firstCustomProps.length; j++) {
                        let firstProp = firstCustomProps[j];
                        let found = false;
                        for (let curProp of props) {
                            if (
                                firstProp.name === curProp.name &&
                                firstProp.isArray === curProp.isArray
                            ) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            rem.push(j);
                        }
                    }

                    rem.forEach(idx => firstCustomProps.splice(idx, 1));
                }
            }
        }

        if (firstProps.length > 0) {
            let items: Item<BreakdownEventCommonProperty, null>[] = [];
            firstProps.forEach(prop =>
                items.push({
                    item: newBreakdownEventCommonProperty(prop.id),
                    name: prop.name
                })
            );

            ret.push({ name: "Event Properties", items: items });
        }

        if (firstCustomProps.length > 0) {
            let items: Item<BreakdownEventCommonProperty, null>[] = [];
            firstCustomProps.forEach(prop =>
                items.push({
                    item: newBreakdownEventCommonProperty(prop.id),
                    name: prop.name
                })
            );

            ret.push({
                name: "Event Custom Properties",
                items: items
            });
        }
    }

    return ret;
});

let selectedItem = computed(() => {
    if (props.selected) {
        return props.selected;
    } else {
        return items?.value[0]?.items[0]?.item;
    }
});

const select = (type: Breakdown) => {
    emit("select", type);
};
</script>
