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
import { PropertyRef } from './property-ref';
import { QueryAggregate } from './query-aggregate';
/**
 * aggregate of property per by group
 * @export
 * @interface QueryAggregatePropertyPerGroup
 */
export interface QueryAggregatePropertyPerGroup {
    /**
     * 
     * @type {QueryAggregate}
     * @memberof QueryAggregatePropertyPerGroup
     */
    aggregate: QueryAggregate;
    /**
     * 
     * @type {QueryAggregate}
     * @memberof QueryAggregatePropertyPerGroup
     */
    aggregatePerGroup: QueryAggregate;
}