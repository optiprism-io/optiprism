/* tslint:disable */
/* eslint-disable */
/**
 * OptiPrism
 * No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)
 *
 * OpenAPI spec version: 1.0.0
 * Contact: api@optiprism.io
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */
import { EventGroupedFiltersGroups } from './event-grouped-filters-groups';
/**
 * event filters
 * @export
 * @interface EventGroupedFilters
 */
export interface EventGroupedFilters {
    /**
     * 
     * @type {string}
     * @memberof EventGroupedFilters
     */
    groupsCondition?: EventGroupedFiltersGroupsConditionEnum;
    /**
     * 
     * @type {Array<EventGroupedFiltersGroups>}
     * @memberof EventGroupedFilters
     */
    groups: Array<EventGroupedFiltersGroups>;
}

/**
    * @export
    * @enum {string}
    */
export enum EventGroupedFiltersGroupsConditionEnum {
    And = 'and',
    Or = 'or'
}

