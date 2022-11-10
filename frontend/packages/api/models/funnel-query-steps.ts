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
import { FunnelEvent } from './funnel-event';
/**
 * 
 * @export
 * @interface FunnelQuerySteps
 */
export interface FunnelQuerySteps {
    /**
     * 
     * @type {Array<FunnelEvent>}
     * @memberof FunnelQuerySteps
     */
    events?: Array<FunnelEvent>;
    /**
     * 
     * @type {string}
     * @memberof FunnelQuerySteps
     */
    order?: FunnelQueryStepsOrderEnum;
}

/**
    * @export
    * @enum {string}
    */
export enum FunnelQueryStepsOrderEnum {
    Any = 'any',
    Exact = 'exact'
}
