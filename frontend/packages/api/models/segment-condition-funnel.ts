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
import { TimeUnit } from './time-unit';
/**
 * 
 * @export
 * @interface SegmentConditionFunnel
 */
export interface SegmentConditionFunnel {
    /**
     * 
     * @type {string}
     * @memberof SegmentConditionFunnel
     */
    type?: SegmentConditionFunnelTypeEnum;
    /**
     * 
     * @type {number}
     * @memberof SegmentConditionFunnel
     */
    last?: number;
    /**
     * 
     * @type {TimeUnit}
     * @memberof SegmentConditionFunnel
     */
    bucket?: TimeUnit;
}

/**
    * @export
    * @enum {string}
    */
export enum SegmentConditionFunnelTypeEnum {
    Funnel = 'funnel'
}
