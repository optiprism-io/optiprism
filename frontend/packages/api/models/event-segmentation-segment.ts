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
import { SegmentCondition } from './segment-condition';
/**
 * segment
 * @export
 * @interface EventSegmentationSegment
 */
export interface EventSegmentationSegment {
    /**
     * name of segment
     * @type {string}
     * @memberof EventSegmentationSegment
     */
    name?: string;
    /**
     * array of conditions
     * @type {Array<SegmentCondition>}
     * @memberof EventSegmentationSegment
     */
    conditions: Array<SegmentCondition>;
}