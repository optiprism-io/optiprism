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
import { BreakdownByProperty } from './breakdown-by-property';
import { EventFilterByProperty } from './event-filter-by-property';
import { EventRef } from './event-ref';
import { QueryAggregateProperty } from './query-aggregate-property';
import { QueryAggregatePropertyPerGroup } from './query-aggregate-property-per-group';
import { QueryCountPerGroup } from './query-count-per-group';
import { QueryFormula } from './query-formula';
import { QuerySimple } from './query-simple';
/**
 * event object
 * @export
 * @interface EventSegmentationEvent
 */
export interface EventSegmentationEvent extends EventRef {
    /**
     * array of event filters
     * @type {Array<EventFilterByProperty>}
     * @memberof EventSegmentationEvent
     */
    filters?: Array<EventFilterByProperty>;
    /**
     * 
     * @type {Array<BreakdownByProperty>}
     * @memberof EventSegmentationEvent
     */
    breakdowns?: Array<BreakdownByProperty>;
    /**
     * array of event queries
     * @type {Array<QuerySimple | QueryCountPerGroup | QueryAggregatePropertyPerGroup | QueryAggregateProperty | QueryFormula>}
     * @memberof EventSegmentationEvent
     */
    queries: Array<QuerySimple | QueryCountPerGroup | QueryAggregatePropertyPerGroup | QueryAggregateProperty | QueryFormula>;
}


