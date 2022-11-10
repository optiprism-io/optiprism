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
import { TimeAfterFirstUse } from './time-after-first-use';
import { TimeBetween } from './time-between';
import { TimeLast } from './time-last';
import { TimeWindowEach } from './time-window-each';
/**
 * find all users who made event X times
 * @export
 * @interface DidEventCount
 */
export interface DidEventCount {
    /**
     * 
     * @type {string}
     * @memberof DidEventCount
     */
    type: DidEventCountTypeEnum;
    /**
     * 
     * @type {PropertyFilterOperation}
     * @memberof DidEventCount
     */
    operation: PropertyFilterOperation;
    /**
     * one or more values. Doesn't need if operation is \"empty\" or \"exist\"
     * @type {number}
     * @memberof DidEventCount
     */
    value?: number;
    /**
     * time frame
     * @type {TimeBetween | TimeLast | TimeAfterFirstUse | TimeWindowEach}
     * @memberof DidEventCount
     */
    time: TimeBetween | TimeLast | TimeAfterFirstUse | TimeWindowEach;
}

/**
    * @export
    * @enum {string}
    */
export enum DidEventCountTypeEnum {
    DidEventCount = 'didEventCount'
}
