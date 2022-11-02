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
import { EventFilterByProperty } from './event-filter-by-property';
import { EventRef } from './event-ref';
/**
 * custom event will matches all of the provided events
 * @export
 * @interface CustomEventEvent
 */
export interface CustomEventEvent extends EventRef {
    /**
     * array of event filters
     * @type {Array<EventFilterByProperty>}
     * @memberof CustomEventEvent
     */
    filters: Array<EventFilterByProperty>;
}


