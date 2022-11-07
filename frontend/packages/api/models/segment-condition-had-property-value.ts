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
import { PropertyFilterOperation } from './property-filter-operation';
import { TimeBetween } from './time-between';
import { TimeLast } from './time-last';
import { TimeWindowEach } from './time-window-each';
import { Value } from './value';
/**
 * check whether the user had a property with a value
 * @export
 * @interface SegmentConditionHadPropertyValue
 */
export interface SegmentConditionHadPropertyValue {
    /**
     * 
     * @type {string}
     * @memberof SegmentConditionHadPropertyValue
     */
    type: SegmentConditionHadPropertyValueTypeEnum;
    /**
     * property name. Because property here is a user only, we don't need propertyType
     * @type {string}
     * @memberof SegmentConditionHadPropertyValue
     */
    propertyName: string;
    /**
     * 
     * @type {PropertyFilterOperation}
     * @memberof SegmentConditionHadPropertyValue
     */
    operation: PropertyFilterOperation;
    /**
     * one or more values. Doesn't need if operation is \"empty\" or \"exist\"
     * @type {Array<Value>}
     * @memberof SegmentConditionHadPropertyValue
     */
    values?: Array<Value>;
    /**
     * time frame
     * @type {TimeBetween | TimeLast | TimeWindowEach}
     * @memberof SegmentConditionHadPropertyValue
     */
    time: TimeBetween | TimeLast | TimeWindowEach;
}

/**
    * @export
    * @enum {string}
    */
export enum SegmentConditionHadPropertyValueTypeEnum {
    HadPropertyValue = 'hadPropertyValue'
}

