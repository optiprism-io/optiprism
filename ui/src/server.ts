import { createServer } from 'miragejs'
import { DataType } from '@/api'
import { BASE_PATH } from '@/api/base'
import { EventStatus, UserProperty, UserCustomProperty } from '@/types/events';
import splineChartMocks from '@/mocks/splineChart.json';
import liveStresmMocks from '@/mocks/reports/liveStream.json'

import eventSegmentationsMocks from '@/mocks/eventSegmentations/eventSegmentations.json';
import eventMocks from '@/mocks/eventSegmentations/events.json';
import eventPropertiesMocks from '@/mocks/eventSegmentations/eventProperties.json';

export default function ({ environment = 'development' } = {}) {
    return createServer({
        seeds(server) {
            server.db.loadData({
                events: eventMocks,
                eventProperties: eventPropertiesMocks,

                customEvents: [{
                    id: 1,
                    name: 'Create Product',
                    events: [{
                        eventId: 3,
                        eventName: 'view_product',
                        eventType: 'regular',
                        filters: []
                    }]
                }, {
                    id: 2,
                    name: 'Find Product',
                    events: [{
                        eventId: 2,
                        eventName: 'search',
                        eventType: 'regular',
                        filters: []
                    }]
                }],
            })
        },

        routes() {
            this.namespace = 'api'
            this.timing = 110

            this.get('/schema/events', (schema) => {
                return schema.db.events
            });

            this.put(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/events/:event_id`, (schema, request) => {
                const customEvent = JSON.parse(request.requestBody)

                return schema.db.events.update(request.params.event_id, customEvent)
            })

            this.get(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/schema/custom-events`, (schema) => {
                return schema.db.customEvents.map(item => ({...item, id: Number(item.id)}))
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

            this.post(`${BASE_PATH}/v1/organizations/:organization_id/projects/:project_id/data/events-stream`, (schema, request) => {
                return liveStresmMocks
            })

            this.get('/schema/event-properties', (schema) => {
                return schema.db.eventProperties
            });

            this.get('/schema/event-custom-properties', () => {
                return [
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
                ];
            });

            this.get('/schema/user-properties', (): UserProperty[] => {
                return [
                    {
                        id: 1,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: 'Name',
                        displayName: 'Name',
                        description: 'Name description',
                        status: EventStatus.Enabled,
                        dataType: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.Number
                    },
                    {
                        id: 2,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: 'Age',
                        displayName: 'Age',
                        description:
                            'Lorem, ipsum dolor sit amet consectetur adipisicing elit. Architecto, temporibus.',
                        status: EventStatus.Enabled,
                        dataType: DataType.Number,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.Number
                    },
                    {
                        id: 3,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: 'Country',
                        displayName: 'Country',
                        description: 'Country description',
                        status: EventStatus.Enabled,
                        dataType: DataType.String,
                        nullable: true,
                        isArray: true,
                        isDictionary: true,
                        dictionaryType: DataType.Number
                    },
                    {
                        id: 4,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: 'Device',
                        displayName: 'Device',
                        description: 'Device description',
                        status: EventStatus.Enabled,
                        dataType: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.Number
                    }
                ];
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

            this.get('/data/property-values', (_, request): string[] => {
                const propertyName = request.queryParams.property_name

                if (propertyName === 'Country') {
                    return ['Spain', 'USA', 'United Kingdom', 'Poland']
                } else {
                    return ['Furniture', 'Doors', 'Lamp', 'Tables', 'Shelves']
                }
            });

            this.get('/chart', (): any[] => {
                return splineChartMocks;
            });

            this.post('/queries/event-segmentation', (_, request) => {
                const body = JSON.parse(request.requestBody);

                if (body.events.length || body.segments) {
                    return eventSegmentationsMocks;
                } else {
                    return {
                        series: []
                    };
                }
            });
        }
    });
}
