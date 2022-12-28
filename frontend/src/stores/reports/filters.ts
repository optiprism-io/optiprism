import {defineStore} from 'pinia';
import {Filter} from '@/types/filters';
import {
    EventGroupedFilters,
    EventGroupedFiltersGroupsConditionEnum,
    EventGroupedFiltersGroupsInnerFiltersInner,
    EventGroupedFiltersGroupsInnerFiltersInnerTypeEnum,
} from '@/api'

export const filterConditions = ['and', 'or'] as const;
export type FilterCondition = typeof filterConditions[number];

export const filterConditionOperations: Record<FilterCondition, string> = {
    and: 'all',
    or: 'any',
}

export interface FilterGroup {
    condition?: EventGroupedFiltersGroupsConditionEnum;
    filters: Filter[];
}

type ChangeFilterGroupConditionPayload = {
    index: number;
    condition: EventGroupedFiltersGroupsConditionEnum;
}

type AddFilterToGroupPayload = {
    index: number;
    filter: Filter;
}

type RemoveFilterFromGroupPayload = {
    index: number;
    filterIndex: number;
}

type EditFilterForGroupPayload = {
    index: number;
    filterIndex: number;
    filter: Partial<Filter>;
}

interface FilterGroupsStore {
    condition: EventGroupedFiltersGroupsConditionEnum;
    filterGroups: FilterGroup[];
}

export const useFilterGroupsStore = defineStore('filter-groups', {
    state: (): FilterGroupsStore => ({
        condition: 'and',
        filterGroups: [
            {
                condition: 'and',
                filters: []
            }
        ]
    }),
    actions: {
        setCondition(payload: EventGroupedFiltersGroupsConditionEnum): void {
            this.condition = payload;
        },
        addFilterGroup(): void {
            this.filterGroups.push({
                condition: 'and',
                filters: []
            })
        },
        removeFilterGroup(index: number) {
            this.filterGroups.splice(index, 1);
        },
        changeFilterGroupCondition(payload: ChangeFilterGroupConditionPayload): void {
            this.filterGroups[payload.index].condition = payload.condition;
        },
        addFilterToGroup(payload: AddFilterToGroupPayload): void {
            this.filterGroups[payload.index].filters.push(payload.filter);
        },
        removeFilterForGroup(payload: RemoveFilterFromGroupPayload): void {
            this.filterGroups[payload.index].filters.splice(payload.filterIndex, 1);
        },
        editFilterForGroup(payload: EditFilterForGroupPayload): void {
            this.filterGroups[payload.index].filters[payload.filterIndex] = {
                ...this.filterGroups[payload.index].filters[payload.filterIndex],
                ...payload.filter
            }
        }
    },
    getters: {
        filters(): EventGroupedFilters {
            return {
                groupsCondition: this.condition,
                groups: this.filterGroups.map(group => {
                    return {
                        filtersCondition: group.condition,
                        filters: group.filters.map((filter): EventGroupedFiltersGroupsInnerFiltersInner => {
                            return {
                                type: 'property' as EventGroupedFiltersGroupsInnerFiltersInnerTypeEnum,
                                cohortId: 0,
                                groupId: 0,
                                propertyType: filter.propRef?.type || 'event',
                                operation: filter.opId,
                                value: filter.values,
                                propertyId: filter.propRef?.id,
                            }
                        })
                    }
                }),
            }
        }
    }
})
