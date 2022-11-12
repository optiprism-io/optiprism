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
import { AnalysisCumulative } from './analysis-cumulative';
import { AnalysisLinear } from './analysis-linear';
import { AnalysisRollingAverage } from './analysis-rolling-average';
import { AnalysisRollingWindow } from './analysis-rolling-window';
import { BreakdownByProperty } from './breakdown-by-property';
import { EventChartType } from './event-chart-type';
import { EventGroupedFilters } from './event-grouped-filters';
import { EventSegmentationCompare } from './event-segmentation-compare';
import { EventSegmentationEvent } from './event-segmentation-event';
import { EventSegmentationSegment } from './event-segmentation-segment';
import { TimeBetween } from './time-between';
import { TimeFrom } from './time-from';
import { TimeLast } from './time-last';
import { TimeUnit } from './time-unit';
/**
 * event segmentation report type main payload
 * @export
 * @interface EventSegmentation
 */
export interface EventSegmentation {
    /**
     * select time
     * @type {TimeBetween | TimeFrom | TimeLast}
     * @memberof EventSegmentation
     */
    time: TimeBetween | TimeFrom | TimeLast;
    /**
     * group that is used in aggregations by group. For instance, group by user or group by organizartion.
     * @type {string}
     * @memberof EventSegmentation
     */
    group: string;
    /**
     * 
     * @type {TimeUnit}
     * @memberof EventSegmentation
     */
    intervalUnit: TimeUnit;
    /**
     * 
     * @type {EventChartType}
     * @memberof EventSegmentation
     */
    chartType: EventChartType;
    /**
     * analysis type
     * @type {AnalysisLinear | AnalysisRollingAverage | AnalysisRollingWindow | AnalysisCumulative}
     * @memberof EventSegmentation
     */
    analysis: AnalysisLinear | AnalysisRollingAverage | AnalysisRollingWindow | AnalysisCumulative;
    /**
     * 
     * @type {EventSegmentationCompare}
     * @memberof EventSegmentation
     */
    compare?: EventSegmentationCompare;
    /**
     * array of events to query
     * @type {Array<EventSegmentationEvent>}
     * @memberof EventSegmentation
     */
    events: Array<EventSegmentationEvent>;
    /**
     * 
     * @type {EventGroupedFilters}
     * @memberof EventSegmentation
     */
    filters?: EventGroupedFilters;
    /**
     * array of common breakdowns (which applies to all events)
     * @type {Array<BreakdownByProperty>}
     * @memberof EventSegmentation
     */
    breakdowns?: Array<BreakdownByProperty>;
    /**
     * array of segments
     * @type {Array<EventSegmentationSegment>}
     * @memberof EventSegmentation
     */
    segments?: Array<EventSegmentationSegment>;
}
