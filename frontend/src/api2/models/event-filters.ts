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
import { EventFiltersGroups } from './event-filters-groups';
/**
 * event filters
 * @export
 * @interface EventFilters
 */
export interface EventFilters {
    /**
     * 
     * @type {string}
     * @memberof EventFilters
     */
    groupsCondition?: EventFiltersGroupsConditionEnum;
    /**
     * 
     * @type {Array<EventFiltersGroups>}
     * @memberof EventFilters
     */
    groups?: Array<EventFiltersGroups>;
}

/**
    * @export
    * @enum {string}
    */
export enum EventFiltersGroupsConditionEnum {
    And = 'and',
    Or = 'or'
}

