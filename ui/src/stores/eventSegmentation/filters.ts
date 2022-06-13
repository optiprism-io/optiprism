import {defineStore} from 'pinia';
import {OperationId, Value} from '@/types';
import {PropertyRef} from '@/types/events';
import {useFilter} from '@/hooks/useFilter';

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
    return <FilterRefCohort>{ type: 'Cohort' };
};

export const newFilterUserProperty = (id: number) => {
    return <FilterRefUserProperty>{ type: 'UserProperty', id: id };
};

export const newFilterUserCustomProperty = (id: number) => {
    return <FilterRefUserCustomProperty>{ type: 'UserCustomProperty', id: id };
};

export const isFilterUserProperty = (ref: FilterRef): boolean => {
    return ref.type === 'UserProperty';
};

export const isFilterUserCustomProperty = (ref: FilterRef): boolean => {
    return ref.type === 'UserCustomProperty';
};

export const isFilterCohort = (ref: FilterRef): boolean => {
    return ref.type === 'Cohort';
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

export const useFiltersStore = defineStore('filters', {
    state: (): Filters => ({ filters: [] }),
    actions: {
        async addFilter(propRef: PropertyRef) {
            const {getValues} = useFilter();

            this.filters.push(<Filter>{
                propRef,
                opId: OperationId.Eq,
                values: [],
                valuesList: await getValues(propRef)
            });
        },
        removeFilter(idx: number): void {
            this.filters.splice(idx, 1);
        },
        async changeFilterRef(filterIdx: number, propRef: PropertyRef) {
            const {getValues} = useFilter();

            this.filters[filterIdx] = <Filter>{
                propRef,
                opId: OperationId.Eq,
                values: [],
                valuesList: await getValues(propRef),
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
