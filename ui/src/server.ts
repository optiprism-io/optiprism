import { createServer, Response } from 'miragejs'
import { DataType, BasicLogin200Response } from '@/api'
import { BASE_PATH } from '@/api/base'
import { EventStatus, UserCustomProperty } from '@/types/events';
import splineChartMocks from '@/mocks/splineChart.json';
import liveStreamMocks from '@/mocks/reports/liveStream.json'
import funnelsMocks from '@/mocks/reports/funnels.json'
import userPropertiesMocks from '@/mocks/eventSegmentations/userProperties.json';
import eventSegmentationsMocks from '@/mocks/eventSegmentations/eventSegmentations.json';
import eventMocks from '@/mocks/eventSegmentations/events.json';
import eventPropertiesMocks from '@/mocks/eventSegmentations/eventProperties.json';
import customEventsMocks from '@/mocks/eventSegmentations/customEvents.json';

const accessToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c'
const refreshToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6Im5pa28ga3VzaCIsImlhdCI6MTUxNjIzOTAyMn0.FzpmXmStgiYEO15ZbwwPafVRQSOCO_xidYjrjRvVIbQ'
const csrfToken = 'CIwNZNlR4XbisJF39I8yWnWX9wX4WFoz'

export default function ({ environment = 'development' } = {}) {
    return createServer({
        seeds(server) {
            server.db.loadData({
                events: eventMocks,
                customEvents: customEventsMocks,
                eventProperties: eventPropertiesMocks,
                userProperties: userPropertiesMocks,
            })
        },

        routes() {
            this.namespace = 'api'
            this.timing = 110

            this.get(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/events`, (schema) => {
                return { events: schema.db.events }
            });

            this.put(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/events/:event_id`, (schema, request) => {
                const customEvent = JSON.parse(request.requestBody)

                return schema.db.events.update(request.params.event_id, customEvent)
            })

            this.get(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/custom-events`, (schema) => {
                return schema.db.customEvents.map(item => ({...item, id: Number(item.id)}))
            })

            this.delete(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/custom-events/:event_id`, (schema, request) => {
                return 'done';
            })

            this.post(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/custom-events`, (schema, request) => {
                const customEvents = JSON.parse(request.requestBody)

                return schema.db.customEvents.insert(customEvents)
            })

            this.put(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/custom-events/:event_id`, (schema, request) => {
                const customEvent = JSON.parse(request.requestBody)
                schema.db.customEvents.update(request.params.event_id, customEvent)

                return schema.db.customEvents
            })

            this.post(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/data/event-records`, (schema, request) => {
                return liveStreamMocks
            })

            this.post(`${BASE_PATH}/organizations/:organization_id/projects/:project_id/reports/funnel`, (schema, request) => {
                return funnelsMocks
            })

            this.get(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/event_properties`, (schema) => {
                return { events: schema.db.eventProperties }
            })

            this.put(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/event_properties/:property_id`, (schema, request) => {
                const property = JSON.parse(request.requestBody)
                return schema.db.eventProperties.update(request.params.property_id, property)
            })

            this.get(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/user_properties`, (schema) => {
                return { events: schema.db.userProperties }
            })

            this.put(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/user_properties/:property_id`, (schema, request) => {
                const property = JSON.parse(request.requestBody)
                return schema.db.userProperties.update(request.params.property_id, property)
            })

            this.get(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/custom-properties`, () => {
                return { events: [
                    {
                        id: 1,
                        eventId: 1,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: 'custom prop 1',
                        dataType: DataType.String,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    },
                    {
                        id: 2,
                        eventId: 1,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: 'custom prop 2',
                        dataType: DataType.String,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    }
                ]};
            });

            this.get('/schema/user-custom-properties', (): UserCustomProperty[] => {
                return [
                    {
                        id: 1,
                        createdBy: 0,
                        createdAt: new Date(),
                        updatedAt: new Date(),
                        updatedBy: 0,
                        projectId: 1,
                        events: [],
                        isSystem: false,
                        isGlobal: true,
                        tags: [],
                        name: 'Custom user prop',
                        displayName: 'Custom user prop',
                        description:
                            'Lorem, ipsum dolor sit amet consectetur adipisicing elit. Architecto, temporibus.',
                        status: EventStatus.Enabled,
                        type: DataType.String,
                        isRequired: false,
                        isArray: false,
                        nullable: false,
                        isDictionary: true,
                        dictionaryType: DataType.Number
                    }
                ];
            });

            this.post(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/data/property-values`, (_, request) => {
                const propertyName = request.queryParams?.property_name
                let values = []

                if (propertyName === 'Country') {
                    values = ['Spain', 'USA', 'United Kingdom', 'Poland']
                } else {
                    values = ['Furniture', 'Doors', 'Lamp', 'Tables', 'Shelves']
                }

                return { values }
            });

            this.get('/chart', (): any[] => {
                return splineChartMocks;
            });

            this.post(`${BASE_PATH}/organizations/:organization_id/projects/:project_id/reports/event-segmentation`, (_, request) => {
                const body = JSON.parse(request.requestBody);

                if (body.events.length || body.segments) {
                    return eventSegmentationsMocks;
                } else {
                    return {
                        columns: []
                    };
                }
            });

            this.post(`${BASE_PATH}/v1/auth/basic/login`, (_, request) => {
                const property = JSON.parse(request.requestBody)

                if (property.email.length <= 5 || property.password.length < 5) {
                    return new Response(400, { some: 'header' }, {
                        'code': '1000_invalid_token',
                        'message': 'string',
                        'fields': {
                            'email': 'Email is too short',
                        }
                    });
                } else {
                    return {
                        accessToken,
                        refreshToken,
                        csrfToken,
                    }
                }
            })

            this.post(`${BASE_PATH}/v1/auth/access`, (): BasicLogin200Response => {
                return {
                    accessToken,
                    refreshToken,
                    csrfToken,
                }
            })
        }
    });
}
