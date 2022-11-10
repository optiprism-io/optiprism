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
import { EventRef } from './event-ref';
import { PropertyFilterOperation } from './property-filter-operation';
import { TimeAfterFirstUse } from './time-after-first-use';
import { TimeBetween } from './time-between';
import { TimeLast } from './time-last';
import { TimeWindowEach } from './time-window-each';
/**
 * find all users who made left event X time more/less than right event.
 * @export
 * @interface DidEventRelativeCount
 */
export interface DidEventRelativeCount {
    /**
     * 
     * @type {string}
     * @memberof DidEventRelativeCount
     */
    type: DidEventRelativeCountTypeEnum;
    /**
     * 
     * @type {PropertyFilterOperation}
     * @memberof DidEventRelativeCount
     */
    operation: PropertyFilterOperation;
    /**
     * 
     * @type {EventRef}
     * @memberof DidEventRelativeCount
     */
    rightEvent: EventRef;
    /**
     * 
     * @type {TimeBetween | TimeLast | TimeAfterFirstUse | TimeWindowEach}
     * @memberof DidEventRelativeCount
     */
    time: TimeBetween | TimeLast | TimeAfterFirstUse | TimeWindowEach;
}

/**
    * @export
    * @enum {string}
    */
export enum DidEventRelativeCountTypeEnum {
    DidEventRelativeCount = 'didEventRelativeCount'
}

