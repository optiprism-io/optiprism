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
import globalAxios, { AxiosResponse, AxiosInstance, AxiosRequestConfig } from 'axios';
import { Configuration } from '../configuration';
// Some imports not used depending on template conditions
// @ts-ignore
import { BASE_PATH, COLLECTION_FORMATS, RequestArgs, BaseAPI, RequiredError } from '../base';
import { CreateReportRequest } from '../models';
import { DataTableResponse } from '../models';
import { ErrorResponse } from '../models';
import { EventSegmentation } from '../models';
import { InlineResponse2001 } from '../models';
import { Report } from '../models';
import { UpdateReportRequest } from '../models';
/**
 * ReportsApi - axios parameter creator
 * @export
 */
export const ReportsApiAxiosParamCreator = function (configuration?: Configuration) {
    return {
        /**
         * 
         * @summary Create report
         * @param {CreateReportRequest} body 
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        createReport: async (body: CreateReportRequest, organizationId: number, projectId: number, options: AxiosRequestConfig = {}): Promise<RequestArgs> => {
            // verify required parameter 'body' is not null or undefined
            if (body === null || body === undefined) {
                throw new RequiredError('body','Required parameter body was null or undefined when calling createReport.');
            }
            // verify required parameter 'organizationId' is not null or undefined
            if (organizationId === null || organizationId === undefined) {
                throw new RequiredError('organizationId','Required parameter organizationId was null or undefined when calling createReport.');
            }
            // verify required parameter 'projectId' is not null or undefined
            if (projectId === null || projectId === undefined) {
                throw new RequiredError('projectId','Required parameter projectId was null or undefined when calling createReport.');
            }
            const localVarPath = `/v1/organizations/{organization_id}/projects/{project_id}/reports`
                .replace(`{${"organization_id"}}`, encodeURIComponent(String(organizationId)))
                .replace(`{${"project_id"}}`, encodeURIComponent(String(projectId)));
            // use dummy base URL string because the URL constructor only accepts absolute URLs.
            const localVarUrlObj = new URL(localVarPath, 'https://example.com');
            let baseOptions;
            if (configuration) {
                baseOptions = configuration.baseOptions;
            }
            const localVarRequestOptions :AxiosRequestConfig = { method: 'POST', ...baseOptions, ...options};
            const localVarHeaderParameter = {} as any;
            const localVarQueryParameter = {} as any;

            // authentication bearerAuth required

            localVarHeaderParameter['Content-Type'] = 'application/json';

            const query = new URLSearchParams(localVarUrlObj.search);
            for (const key in localVarQueryParameter) {
                query.set(key, localVarQueryParameter[key]);
            }
            for (const key in options.params) {
                query.set(key, options.params[key]);
            }
            localVarUrlObj.search = (new URLSearchParams(query)).toString();
            let headersFromBaseOptions = baseOptions && baseOptions.headers ? baseOptions.headers : {};
            localVarRequestOptions.headers = {...localVarHeaderParameter, ...headersFromBaseOptions, ...options.headers};
            const needsSerialization = (typeof body !== "string") || localVarRequestOptions.headers['Content-Type'] === 'application/json';
            localVarRequestOptions.data =  needsSerialization ? JSON.stringify(body !== undefined ? body : {}) : (body || "");

            return {
                url: localVarUrlObj.pathname + localVarUrlObj.search + localVarUrlObj.hash,
                options: localVarRequestOptions,
            };
        },
        /**
         * 
         * @summary Delete report
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        deleteReport: async (organizationId: number, projectId: number, reportId: number, options: AxiosRequestConfig = {}): Promise<RequestArgs> => {
            // verify required parameter 'organizationId' is not null or undefined
            if (organizationId === null || organizationId === undefined) {
                throw new RequiredError('organizationId','Required parameter organizationId was null or undefined when calling deleteReport.');
            }
            // verify required parameter 'projectId' is not null or undefined
            if (projectId === null || projectId === undefined) {
                throw new RequiredError('projectId','Required parameter projectId was null or undefined when calling deleteReport.');
            }
            // verify required parameter 'reportId' is not null or undefined
            if (reportId === null || reportId === undefined) {
                throw new RequiredError('reportId','Required parameter reportId was null or undefined when calling deleteReport.');
            }
            const localVarPath = `/v1/organizations/{organization_id}/projects/{project_id}/reports/{report_id}`
                .replace(`{${"organization_id"}}`, encodeURIComponent(String(organizationId)))
                .replace(`{${"project_id"}}`, encodeURIComponent(String(projectId)))
                .replace(`{${"report_id"}}`, encodeURIComponent(String(reportId)));
            // use dummy base URL string because the URL constructor only accepts absolute URLs.
            const localVarUrlObj = new URL(localVarPath, 'https://example.com');
            let baseOptions;
            if (configuration) {
                baseOptions = configuration.baseOptions;
            }
            const localVarRequestOptions :AxiosRequestConfig = { method: 'DELETE', ...baseOptions, ...options};
            const localVarHeaderParameter = {} as any;
            const localVarQueryParameter = {} as any;

            // authentication bearerAuth required

            const query = new URLSearchParams(localVarUrlObj.search);
            for (const key in localVarQueryParameter) {
                query.set(key, localVarQueryParameter[key]);
            }
            for (const key in options.params) {
                query.set(key, options.params[key]);
            }
            localVarUrlObj.search = (new URLSearchParams(query)).toString();
            let headersFromBaseOptions = baseOptions && baseOptions.headers ? baseOptions.headers : {};
            localVarRequestOptions.headers = {...localVarHeaderParameter, ...headersFromBaseOptions, ...options.headers};

            return {
                url: localVarUrlObj.pathname + localVarUrlObj.search + localVarUrlObj.hash,
                options: localVarRequestOptions,
            };
        },
        /**
         * 
         * @summary Event segmentation query
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {EventSegmentation} [body] 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        eventSegmentationQuery: async (organizationId: number, projectId: number, body?: EventSegmentation, options: AxiosRequestConfig = {}): Promise<RequestArgs> => {
            // verify required parameter 'organizationId' is not null or undefined
            if (organizationId === null || organizationId === undefined) {
                throw new RequiredError('organizationId','Required parameter organizationId was null or undefined when calling eventSegmentationQuery.');
            }
            // verify required parameter 'projectId' is not null or undefined
            if (projectId === null || projectId === undefined) {
                throw new RequiredError('projectId','Required parameter projectId was null or undefined when calling eventSegmentationQuery.');
            }
            const localVarPath = `/organizations/{organization_id}/projects/{project_id}/reports/event-segmentation`
                .replace(`{${"organization_id"}}`, encodeURIComponent(String(organizationId)))
                .replace(`{${"project_id"}}`, encodeURIComponent(String(projectId)));
            // use dummy base URL string because the URL constructor only accepts absolute URLs.
            const localVarUrlObj = new URL(localVarPath, 'https://example.com');
            let baseOptions;
            if (configuration) {
                baseOptions = configuration.baseOptions;
            }
            const localVarRequestOptions :AxiosRequestConfig = { method: 'POST', ...baseOptions, ...options};
            const localVarHeaderParameter = {} as any;
            const localVarQueryParameter = {} as any;

            // authentication bearerAuth required

            localVarHeaderParameter['Content-Type'] = 'application/json';

            const query = new URLSearchParams(localVarUrlObj.search);
            for (const key in localVarQueryParameter) {
                query.set(key, localVarQueryParameter[key]);
            }
            for (const key in options.params) {
                query.set(key, options.params[key]);
            }
            localVarUrlObj.search = (new URLSearchParams(query)).toString();
            let headersFromBaseOptions = baseOptions && baseOptions.headers ? baseOptions.headers : {};
            localVarRequestOptions.headers = {...localVarHeaderParameter, ...headersFromBaseOptions, ...options.headers};
            const needsSerialization = (typeof body !== "string") || localVarRequestOptions.headers['Content-Type'] === 'application/json';
            localVarRequestOptions.data =  needsSerialization ? JSON.stringify(body !== undefined ? body : {}) : (body || "");

            return {
                url: localVarUrlObj.pathname + localVarUrlObj.search + localVarUrlObj.hash,
                options: localVarRequestOptions,
            };
        },
        /**
         * 
         * @summary Get report
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        getReport: async (organizationId: number, projectId: number, reportId: number, options: AxiosRequestConfig = {}): Promise<RequestArgs> => {
            // verify required parameter 'organizationId' is not null or undefined
            if (organizationId === null || organizationId === undefined) {
                throw new RequiredError('organizationId','Required parameter organizationId was null or undefined when calling getReport.');
            }
            // verify required parameter 'projectId' is not null or undefined
            if (projectId === null || projectId === undefined) {
                throw new RequiredError('projectId','Required parameter projectId was null or undefined when calling getReport.');
            }
            // verify required parameter 'reportId' is not null or undefined
            if (reportId === null || reportId === undefined) {
                throw new RequiredError('reportId','Required parameter reportId was null or undefined when calling getReport.');
            }
            const localVarPath = `/v1/organizations/{organization_id}/projects/{project_id}/reports/{report_id}`
                .replace(`{${"organization_id"}}`, encodeURIComponent(String(organizationId)))
                .replace(`{${"project_id"}}`, encodeURIComponent(String(projectId)))
                .replace(`{${"report_id"}}`, encodeURIComponent(String(reportId)));
            // use dummy base URL string because the URL constructor only accepts absolute URLs.
            const localVarUrlObj = new URL(localVarPath, 'https://example.com');
            let baseOptions;
            if (configuration) {
                baseOptions = configuration.baseOptions;
            }
            const localVarRequestOptions :AxiosRequestConfig = { method: 'GET', ...baseOptions, ...options};
            const localVarHeaderParameter = {} as any;
            const localVarQueryParameter = {} as any;

            // authentication bearerAuth required

            const query = new URLSearchParams(localVarUrlObj.search);
            for (const key in localVarQueryParameter) {
                query.set(key, localVarQueryParameter[key]);
            }
            for (const key in options.params) {
                query.set(key, options.params[key]);
            }
            localVarUrlObj.search = (new URLSearchParams(query)).toString();
            let headersFromBaseOptions = baseOptions && baseOptions.headers ? baseOptions.headers : {};
            localVarRequestOptions.headers = {...localVarHeaderParameter, ...headersFromBaseOptions, ...options.headers};

            return {
                url: localVarUrlObj.pathname + localVarUrlObj.search + localVarUrlObj.hash,
                options: localVarRequestOptions,
            };
        },
        /**
         * 
         * @summary Reports list
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        reportsList: async (organizationId: number, projectId: number, options: AxiosRequestConfig = {}): Promise<RequestArgs> => {
            // verify required parameter 'organizationId' is not null or undefined
            if (organizationId === null || organizationId === undefined) {
                throw new RequiredError('organizationId','Required parameter organizationId was null or undefined when calling reportsList.');
            }
            // verify required parameter 'projectId' is not null or undefined
            if (projectId === null || projectId === undefined) {
                throw new RequiredError('projectId','Required parameter projectId was null or undefined when calling reportsList.');
            }
            const localVarPath = `/v1/organizations/{organization_id}/projects/{project_id}/reports`
                .replace(`{${"organization_id"}}`, encodeURIComponent(String(organizationId)))
                .replace(`{${"project_id"}}`, encodeURIComponent(String(projectId)));
            // use dummy base URL string because the URL constructor only accepts absolute URLs.
            const localVarUrlObj = new URL(localVarPath, 'https://example.com');
            let baseOptions;
            if (configuration) {
                baseOptions = configuration.baseOptions;
            }
            const localVarRequestOptions :AxiosRequestConfig = { method: 'GET', ...baseOptions, ...options};
            const localVarHeaderParameter = {} as any;
            const localVarQueryParameter = {} as any;

            // authentication bearerAuth required

            const query = new URLSearchParams(localVarUrlObj.search);
            for (const key in localVarQueryParameter) {
                query.set(key, localVarQueryParameter[key]);
            }
            for (const key in options.params) {
                query.set(key, options.params[key]);
            }
            localVarUrlObj.search = (new URLSearchParams(query)).toString();
            let headersFromBaseOptions = baseOptions && baseOptions.headers ? baseOptions.headers : {};
            localVarRequestOptions.headers = {...localVarHeaderParameter, ...headersFromBaseOptions, ...options.headers};

            return {
                url: localVarUrlObj.pathname + localVarUrlObj.search + localVarUrlObj.hash,
                options: localVarRequestOptions,
            };
        },
        /**
         * 
         * @summary Update report
         * @param {UpdateReportRequest} body 
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        updateReport: async (body: UpdateReportRequest, organizationId: number, projectId: number, reportId: number, options: AxiosRequestConfig = {}): Promise<RequestArgs> => {
            // verify required parameter 'body' is not null or undefined
            if (body === null || body === undefined) {
                throw new RequiredError('body','Required parameter body was null or undefined when calling updateReport.');
            }
            // verify required parameter 'organizationId' is not null or undefined
            if (organizationId === null || organizationId === undefined) {
                throw new RequiredError('organizationId','Required parameter organizationId was null or undefined when calling updateReport.');
            }
            // verify required parameter 'projectId' is not null or undefined
            if (projectId === null || projectId === undefined) {
                throw new RequiredError('projectId','Required parameter projectId was null or undefined when calling updateReport.');
            }
            // verify required parameter 'reportId' is not null or undefined
            if (reportId === null || reportId === undefined) {
                throw new RequiredError('reportId','Required parameter reportId was null or undefined when calling updateReport.');
            }
            const localVarPath = `/v1/organizations/{organization_id}/projects/{project_id}/reports/{report_id}`
                .replace(`{${"organization_id"}}`, encodeURIComponent(String(organizationId)))
                .replace(`{${"project_id"}}`, encodeURIComponent(String(projectId)))
                .replace(`{${"report_id"}}`, encodeURIComponent(String(reportId)));
            // use dummy base URL string because the URL constructor only accepts absolute URLs.
            const localVarUrlObj = new URL(localVarPath, 'https://example.com');
            let baseOptions;
            if (configuration) {
                baseOptions = configuration.baseOptions;
            }
            const localVarRequestOptions :AxiosRequestConfig = { method: 'PUT', ...baseOptions, ...options};
            const localVarHeaderParameter = {} as any;
            const localVarQueryParameter = {} as any;

            // authentication bearerAuth required

            localVarHeaderParameter['Content-Type'] = 'application/json';

            const query = new URLSearchParams(localVarUrlObj.search);
            for (const key in localVarQueryParameter) {
                query.set(key, localVarQueryParameter[key]);
            }
            for (const key in options.params) {
                query.set(key, options.params[key]);
            }
            localVarUrlObj.search = (new URLSearchParams(query)).toString();
            let headersFromBaseOptions = baseOptions && baseOptions.headers ? baseOptions.headers : {};
            localVarRequestOptions.headers = {...localVarHeaderParameter, ...headersFromBaseOptions, ...options.headers};
            const needsSerialization = (typeof body !== "string") || localVarRequestOptions.headers['Content-Type'] === 'application/json';
            localVarRequestOptions.data =  needsSerialization ? JSON.stringify(body !== undefined ? body : {}) : (body || "");

            return {
                url: localVarUrlObj.pathname + localVarUrlObj.search + localVarUrlObj.hash,
                options: localVarRequestOptions,
            };
        },
    }
};

/**
 * ReportsApi - functional programming interface
 * @export
 */
export const ReportsApiFp = function(configuration?: Configuration) {
    return {
        /**
         * 
         * @summary Create report
         * @param {CreateReportRequest} body 
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async createReport(body: CreateReportRequest, organizationId: number, projectId: number, options?: AxiosRequestConfig): Promise<(axios?: AxiosInstance, basePath?: string) => Promise<AxiosResponse<Report>>> {
            const localVarAxiosArgs = await ReportsApiAxiosParamCreator(configuration).createReport(body, organizationId, projectId, options);
            return (axios: AxiosInstance = globalAxios, basePath: string = BASE_PATH) => {
                const axiosRequestArgs :AxiosRequestConfig = {...localVarAxiosArgs.options, url: basePath + localVarAxiosArgs.url};
                return axios.request(axiosRequestArgs);
            };
        },
        /**
         * 
         * @summary Delete report
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async deleteReport(organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig): Promise<(axios?: AxiosInstance, basePath?: string) => Promise<AxiosResponse<void>>> {
            const localVarAxiosArgs = await ReportsApiAxiosParamCreator(configuration).deleteReport(organizationId, projectId, reportId, options);
            return (axios: AxiosInstance = globalAxios, basePath: string = BASE_PATH) => {
                const axiosRequestArgs :AxiosRequestConfig = {...localVarAxiosArgs.options, url: basePath + localVarAxiosArgs.url};
                return axios.request(axiosRequestArgs);
            };
        },
        /**
         * 
         * @summary Event segmentation query
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {EventSegmentation} [body] 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async eventSegmentationQuery(organizationId: number, projectId: number, body?: EventSegmentation, options?: AxiosRequestConfig): Promise<(axios?: AxiosInstance, basePath?: string) => Promise<AxiosResponse<DataTableResponse>>> {
            const localVarAxiosArgs = await ReportsApiAxiosParamCreator(configuration).eventSegmentationQuery(organizationId, projectId, body, options);
            return (axios: AxiosInstance = globalAxios, basePath: string = BASE_PATH) => {
                const axiosRequestArgs :AxiosRequestConfig = {...localVarAxiosArgs.options, url: basePath + localVarAxiosArgs.url};
                return axios.request(axiosRequestArgs);
            };
        },
        /**
         * 
         * @summary Get report
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async getReport(organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig): Promise<(axios?: AxiosInstance, basePath?: string) => Promise<AxiosResponse<Report>>> {
            const localVarAxiosArgs = await ReportsApiAxiosParamCreator(configuration).getReport(organizationId, projectId, reportId, options);
            return (axios: AxiosInstance = globalAxios, basePath: string = BASE_PATH) => {
                const axiosRequestArgs :AxiosRequestConfig = {...localVarAxiosArgs.options, url: basePath + localVarAxiosArgs.url};
                return axios.request(axiosRequestArgs);
            };
        },
        /**
         * 
         * @summary Reports list
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async reportsList(organizationId: number, projectId: number, options?: AxiosRequestConfig): Promise<(axios?: AxiosInstance, basePath?: string) => Promise<AxiosResponse<InlineResponse2001>>> {
            const localVarAxiosArgs = await ReportsApiAxiosParamCreator(configuration).reportsList(organizationId, projectId, options);
            return (axios: AxiosInstance = globalAxios, basePath: string = BASE_PATH) => {
                const axiosRequestArgs :AxiosRequestConfig = {...localVarAxiosArgs.options, url: basePath + localVarAxiosArgs.url};
                return axios.request(axiosRequestArgs);
            };
        },
        /**
         * 
         * @summary Update report
         * @param {UpdateReportRequest} body 
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async updateReport(body: UpdateReportRequest, organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig): Promise<(axios?: AxiosInstance, basePath?: string) => Promise<AxiosResponse<Report>>> {
            const localVarAxiosArgs = await ReportsApiAxiosParamCreator(configuration).updateReport(body, organizationId, projectId, reportId, options);
            return (axios: AxiosInstance = globalAxios, basePath: string = BASE_PATH) => {
                const axiosRequestArgs :AxiosRequestConfig = {...localVarAxiosArgs.options, url: basePath + localVarAxiosArgs.url};
                return axios.request(axiosRequestArgs);
            };
        },
    }
};

/**
 * ReportsApi - factory interface
 * @export
 */
export const ReportsApiFactory = function (configuration?: Configuration, basePath?: string, axios?: AxiosInstance) {
    return {
        /**
         * 
         * @summary Create report
         * @param {CreateReportRequest} body 
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async createReport(body: CreateReportRequest, organizationId: number, projectId: number, options?: AxiosRequestConfig): Promise<AxiosResponse<Report>> {
            return ReportsApiFp(configuration).createReport(body, organizationId, projectId, options).then((request) => request(axios, basePath));
        },
        /**
         * 
         * @summary Delete report
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async deleteReport(organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig): Promise<AxiosResponse<void>> {
            return ReportsApiFp(configuration).deleteReport(organizationId, projectId, reportId, options).then((request) => request(axios, basePath));
        },
        /**
         * 
         * @summary Event segmentation query
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {EventSegmentation} [body] 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async eventSegmentationQuery(organizationId: number, projectId: number, body?: EventSegmentation, options?: AxiosRequestConfig): Promise<AxiosResponse<DataTableResponse>> {
            return ReportsApiFp(configuration).eventSegmentationQuery(organizationId, projectId, body, options).then((request) => request(axios, basePath));
        },
        /**
         * 
         * @summary Get report
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async getReport(organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig): Promise<AxiosResponse<Report>> {
            return ReportsApiFp(configuration).getReport(organizationId, projectId, reportId, options).then((request) => request(axios, basePath));
        },
        /**
         * 
         * @summary Reports list
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async reportsList(organizationId: number, projectId: number, options?: AxiosRequestConfig): Promise<AxiosResponse<InlineResponse2001>> {
            return ReportsApiFp(configuration).reportsList(organizationId, projectId, options).then((request) => request(axios, basePath));
        },
        /**
         * 
         * @summary Update report
         * @param {UpdateReportRequest} body 
         * @param {number} organizationId 
         * @param {number} projectId 
         * @param {number} reportId Report ID
         * @param {*} [options] Override http request option.
         * @throws {RequiredError}
         */
        async updateReport(body: UpdateReportRequest, organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig): Promise<AxiosResponse<Report>> {
            return ReportsApiFp(configuration).updateReport(body, organizationId, projectId, reportId, options).then((request) => request(axios, basePath));
        },
    };
};

/**
 * ReportsApi - object-oriented interface
 * @export
 * @class ReportsApi
 * @extends {BaseAPI}
 */
export class ReportsApi extends BaseAPI {
    /**
     * 
     * @summary Create report
     * @param {CreateReportRequest} body 
     * @param {number} organizationId 
     * @param {number} projectId 
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof ReportsApi
     */
    public async createReport(body: CreateReportRequest, organizationId: number, projectId: number, options?: AxiosRequestConfig) : Promise<AxiosResponse<Report>> {
        return ReportsApiFp(this.configuration).createReport(body, organizationId, projectId, options).then((request) => request(this.axios, this.basePath));
    }
    /**
     * 
     * @summary Delete report
     * @param {number} organizationId 
     * @param {number} projectId 
     * @param {number} reportId Report ID
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof ReportsApi
     */
    public async deleteReport(organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig) : Promise<AxiosResponse<void>> {
        return ReportsApiFp(this.configuration).deleteReport(organizationId, projectId, reportId, options).then((request) => request(this.axios, this.basePath));
    }
    /**
     * 
     * @summary Event segmentation query
     * @param {number} organizationId 
     * @param {number} projectId 
     * @param {EventSegmentation} [body] 
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof ReportsApi
     */
    public async eventSegmentationQuery(organizationId: number, projectId: number, body?: EventSegmentation, options?: AxiosRequestConfig) : Promise<AxiosResponse<DataTableResponse>> {
        return ReportsApiFp(this.configuration).eventSegmentationQuery(organizationId, projectId, body, options).then((request) => request(this.axios, this.basePath));
    }
    /**
     * 
     * @summary Get report
     * @param {number} organizationId 
     * @param {number} projectId 
     * @param {number} reportId Report ID
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof ReportsApi
     */
    public async getReport(organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig) : Promise<AxiosResponse<Report>> {
        return ReportsApiFp(this.configuration).getReport(organizationId, projectId, reportId, options).then((request) => request(this.axios, this.basePath));
    }
    /**
     * 
     * @summary Reports list
     * @param {number} organizationId 
     * @param {number} projectId 
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof ReportsApi
     */
    public async reportsList(organizationId: number, projectId: number, options?: AxiosRequestConfig) : Promise<AxiosResponse<InlineResponse2001>> {
        return ReportsApiFp(this.configuration).reportsList(organizationId, projectId, options).then((request) => request(this.axios, this.basePath));
    }
    /**
     * 
     * @summary Update report
     * @param {UpdateReportRequest} body 
     * @param {number} organizationId 
     * @param {number} projectId 
     * @param {number} reportId Report ID
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof ReportsApi
     */
    public async updateReport(body: UpdateReportRequest, organizationId: number, projectId: number, reportId: number, options?: AxiosRequestConfig) : Promise<AxiosResponse<Report>> {
        return ReportsApiFp(this.configuration).updateReport(body, organizationId, projectId, reportId, options).then((request) => request(this.axios, this.basePath));
    }
}