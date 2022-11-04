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
import { CustomEventEvent } from './custom-event-event';
/**
 * Custom Event is an alias to an expression which is used while querying. You can use regular or custom events in expression. You can combine events in expression, you can use filter by properties. 
 * @export
 * @interface CustomEvent
 */
export interface CustomEvent {
    /**
     * 
     * @type {number}
     * @memberof CustomEvent
     */
    id?: number;
    /**
     * 
     * @type {Date}
     * @memberof CustomEvent
     */
    createdAt?: Date;
    /**
     * 
     * @type {Date}
     * @memberof CustomEvent
     */
    updatedAt?: Date;
    /**
     * 
     * @type {number}
     * @memberof CustomEvent
     */
    createdBy?: number;
    /**
     * 
     * @type {number}
     * @memberof CustomEvent
     */
    updatedBy?: number;
    /**
     * 
     * @type {number}
     * @memberof CustomEvent
     */
    projectId?: number;
    /**
     * 
     * @type {boolean}
     * @memberof CustomEvent
     */
    isSystem?: boolean;
    /**
     * 
     * @type {string}
     * @memberof CustomEvent
     */
    status?: CustomEventStatusEnum;
    /**
     * 
     * @type {string}
     * @memberof CustomEvent
     */
    name?: string;
    /**
     * 
     * @type {string}
     * @memberof CustomEvent
     */
    description?: string;
    /**
     * 
     * @type {Array<string>}
     * @memberof CustomEvent
     */
    tags?: Array<string>;
    /**
     * 
     * @type {Array<CustomEventEvent>}
     * @memberof CustomEvent
     */
    events?: Array<CustomEventEvent>;
}

/**
    * @export
    * @enum {string}
    */
export enum CustomEventStatusEnum {
    Enabled = 'enabled',
    Disabled = 'disabled'
}

