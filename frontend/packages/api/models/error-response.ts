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
/**
 * 
 * @export
 * @interface ErrorResponse
 */
export interface ErrorResponse {
    /**
     * 
     * @type {string}
     * @memberof ErrorResponse
     */
    code?: ErrorResponseCodeEnum;
    /**
     * 
     * @type {string}
     * @memberof ErrorResponse
     */
    message?: string;
    /**
     * 
     * @type {{ [key: string]: string; }}
     * @memberof ErrorResponse
     */
    fields?: { [key: string]: string; };
}

/**
    * @export
    * @enum {string}
    */
export enum ErrorResponseCodeEnum {
    _1000InvalidToken = '1000_invalid_token'
}

