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
import { TimeUnit } from './time-unit';
/**
 * 
 * @export
 * @interface FunnelTimeToConvertChartType
 */
export interface FunnelTimeToConvertChartType {
    /**
     * 
     * @type {string}
     * @memberof FunnelTimeToConvertChartType
     */
    type?: FunnelTimeToConvertChartTypeTypeEnum;
    /**
     * 
     * @type {TimeUnit}
     * @memberof FunnelTimeToConvertChartType
     */
    intervalUnit?: TimeUnit;
    /**
     * 
     * @type {number}
     * @memberof FunnelTimeToConvertChartType
     */
    minInterval?: number;
    /**
     * 
     * @type {number}
     * @memberof FunnelTimeToConvertChartType
     */
    maxInterval?: number;
}

/**
    * @export
    * @enum {string}
    */
export enum FunnelTimeToConvertChartTypeTypeEnum {
    TimeToConvert = 'timeToConvert'
}
