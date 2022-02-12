import { defineStore } from "pinia";
import { OperationId, Value } from "@/types";
import { PropertyRef, PropertyType, EventType, EventRef } from "@/types/events";
import { useLexiconStore } from "@/stores/lexicon";
import schemaService from "@/api/services/schema.service";

export type FilterRefUserProperty = {
    type: string;
    id: number;
};

export type FilterRefUserCustomProperty = {
    type: string;
    id: number;
};

export type FilterRefCohort = {
    type: string;
};

export type FilterRef = FilterRefUserProperty | FilterRefUserCustomProperty | FilterRefCohort;

export const newFilterCohort = () => {
    return <FilterRefCohort>{ type: "Cohort" };
};

export const newFilterUserProperty = (id: number) => {
    return <FilterRefUserProperty>{ type: "UserProperty", id: id };
};

export const newFilterUserCustomProperty = (id: number) => {
    return <FilterRefUserCustomProperty>{ type: "UserCustomProperty", id: id };
};

export const isFilterUserProperty = (ref: FilterRef): boolean => {
    return ref.type === "UserProperty";
};

export const isFilterUserCustomProperty = (ref: FilterRef): boolean => {
    return ref.type === "UserCustomProperty";
};

export const isFilterCohort = (ref: FilterRef): boolean => {
    return ref.type === "Cohort";
};

export interface Filter {
    propRef?: PropertyRef;
    opId: OperationId;
    values: Value[];
    valuesList: string[];
}

type Filters = {
    filters: Filter[];
};

export const useFiltersStore = defineStore("filters", {
    state: (): Filters => ({ filters: [] }),
    getters: {
        eventForValues() {
            const lexiconStore = useLexiconStore();

            return (id: number): EventRef | undefined => {
                const event = lexiconStore.events.find(item => {
                    if (item.properties) {
                        return item.properties.includes(id);
                    }
                });

                let eventRef;

                lexiconStore.eventsList.forEach(item => {
                    const eventStoreRef: any = item.items.find(itemInner => itemInner.item.id === event?.id);

                    if (event) {
                        eventRef = eventStoreRef;
                    }
                })
                return eventRef
            };
        },
    },
    actions: {
        async addFilter(propRef: PropertyRef) {
            const lexiconStore = useLexiconStore();
            let valuesList: string[] = [];
            const eventRef = this.eventForValues(propRef.id);

            try {
                const res = await schemaService.propertiesValues({
                    event_name: eventRef ? lexiconStore.eventName(eventRef) : "",
                    event_type: eventRef ? EventType[eventRef.type] : "",
                    property_name: lexiconStore.propertyName(propRef),
                    property_type: PropertyType[propRef.type]
                });
                if (res) {
                    valuesList = res;
                }
            } catch (error) {
                throw new Error("error getEventsValues");
            }

            this.filters.push(<Filter>{
                propRef,
                opId: OperationId.Eq,
                values: [],
                valuesList: valuesList,
            });
        },
        removeFilter(idx: number): void {
            this.filters.splice(idx, 1);
        },
        async changeFilterRef(filterIdx: number, propRef: PropertyRef) {
            const lexiconStore = useLexiconStore();
            let valuesList: string[] = [];
            const eventRef = this.eventForValues(propRef.id);

            try {
                const res = await schemaService.propertiesValues({
                    event_name: eventRef ? lexiconStore.eventName(eventRef) : "",
                    event_type: eventRef ? EventType[eventRef.type] : "",
                    property_name: lexiconStore.propertyName(propRef),
                    property_type: PropertyType[propRef.type]
                });
                if (res) {
                    valuesList = res;
                }
            } catch (error) {
                throw new Error("error getEventsValues");
            }

            this.filters[filterIdx] = <Filter>{
                propRef,
                opId: OperationId.Eq,
                values: [],
                valuesList: valuesList,
            };
        },
        changeFilterOperation(filterIdx: number, opId: OperationId): void {
            this.filters[filterIdx].opId = opId;
        },
        addFilterValue(filterIdx: number, value: Value): void {
            this.filters[filterIdx].values.push(value);
        },
        removeFilterValue(filterIdx: number, value: Value): void {
            this.filters[filterIdx].values = this.filters[filterIdx].values.filter(v => {
                return v !== value;
            });
        }
    }
});
