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
import { EventGroupedFilters } from './event-grouped-filters';
import { EventRecordsListRequestSearch } from './event-records-list-request-search';
import { EventRef } from './event-ref';
import { TimeBetween } from './time-between';
import { TimeFrom } from './time-from';
import { TimeLast } from './time-last';
/**
 * request event records sorted by time of creation
 * @export
 * @interface EventRecordsListRequest
 */
export interface EventRecordsListRequest {
    /**
     * select time
     * @type {TimeBetween | TimeFrom | TimeLast}
     * @memberof EventRecordsListRequest
     */
    time: TimeBetween | TimeFrom | TimeLast;
    /**
     * group that is used in aggregations by group. For instance, group by user or group by organization.
     * @type {string}
     * @memberof EventRecordsListRequest
     */
    group: string;
    /**
     * 
     * @type {EventRecordsListRequestSearch}
     * @memberof EventRecordsListRequest
     */
    search?: EventRecordsListRequestSearch;
    /**
     * array of events to query
     * @type {Array<EventRef & any>}
     * @memberof EventRecordsListRequest
     */
    events?: Array<EventRef & any>;
    /**
     * 
     * @type {EventGroupedFilters}
     * @memberof EventRecordsListRequest
     */
    filters?: EventGroupedFilters;
}
