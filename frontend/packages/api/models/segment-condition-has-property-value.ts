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
import { Value } from './value';
/**
 * check whether the user currently has a property with a value
 * @export
 * @interface SegmentConditionHasPropertyValue
 */
export interface SegmentConditionHasPropertyValue {
    /**
     * 
     * @type {string}
     * @memberof SegmentConditionHasPropertyValue
     */
    type: SegmentConditionHasPropertyValueTypeEnum;
    /**
     * property name. Because property here is a user only, we don't need propertyType
     * @type {string}
     * @memberof SegmentConditionHasPropertyValue
     */
    propertyName: string;
    /**
     * 
     * @type {PropertyFilterOperation}
     * @memberof SegmentConditionHasPropertyValue
     */
    operation: PropertyFilterOperation;
    /**
     * one or more values. Doesn't need if operation is \"empty\" or \"exist\"
     * @type {Array<Value>}
     * @memberof SegmentConditionHasPropertyValue
     */
    value?: Array<Value>;
}

/**
    * @export
    * @enum {string}
    */
export enum SegmentConditionHasPropertyValueTypeEnum {
    HasPropertyValue = 'hasPropertyValue'
}

