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
import { EventFilters } from './event-filters';
import { EventSegmentationSegment } from './event-segmentation-segment';
import { GroupRecordsListRequestSearch } from './group-records-list-request-search';
import { TimeBetween } from './time-between';
import { TimeFrom } from './time-from';
import { TimeLast } from './time-last';
/**
 * request group records sorted by time of creation
 * @export
 * @interface GroupRecordsListRequest
 */
export interface GroupRecordsListRequest {
    /**
     * select time
     * @type {TimeBetween | TimeFrom | TimeLast}
     * @memberof GroupRecordsListRequest
     */
    time: TimeBetween | TimeFrom | TimeLast;
    /**
     * group that is used in aggregations by group. For instance, group by user or group by organization.
     * @type {string}
     * @memberof GroupRecordsListRequest
     */
    group: string;
    /**
     * 
     * @type {GroupRecordsListRequestSearch}
     * @memberof GroupRecordsListRequest
     */
    search?: GroupRecordsListRequestSearch;
    /**
     * array of segments
     * @type {Array<EventSegmentationSegment>}
     * @memberof GroupRecordsListRequest
     */
    segments?: Array<EventSegmentationSegment>;
    /**
     * 
     * @type {EventFilters}
     * @memberof GroupRecordsListRequest
     */
    filters?: EventFilters;
}
