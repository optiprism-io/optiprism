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
import { Value } from './value';
/**
 * search in results
 * @export
 * @interface EventRecordsListRequestSearch
 */
export interface EventRecordsListRequestSearch {
    /**
     * 
     * @type {string}
     * @memberof EventRecordsListRequestSearch
     */
    term?: string;
    /**
     * 
     * @type {Array<Value>}
     * @memberof EventRecordsListRequestSearch
     */
    inEventProperties?: Array<Value>;
    /**
     * 
     * @type {Array<Value>}
     * @memberof EventRecordsListRequestSearch
     */
    inUserProperties?: Array<Value>;
}
