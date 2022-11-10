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
import { EventRecordMatchedCustomEvents } from './event-record-matched-custom-events';
/**
 * 
 * @export
 * @interface EventRecord
 */
export interface EventRecord {
    /**
     * 
     * @type {number}
     * @memberof EventRecord
     */
    id?: number;
    /**
     * 
     * @type {string}
     * @memberof EventRecord
     */
    name?: string;
    /**
     * map of property name and property value pairs
     * @type {any}
     * @memberof EventRecord
     */
    properties?: any;
    /**
     * map of user name and property value pairs
     * @type {any}
     * @memberof EventRecord
     */
    userProperties?: any;
    /**
     * 
     * @type {Array<EventRecordMatchedCustomEvents>}
     * @memberof EventRecord
     */
    matchedCustomEvents?: Array<EventRecordMatchedCustomEvents>;
}