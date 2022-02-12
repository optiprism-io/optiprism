import { createServer } from "miragejs";
import { DataType } from "@/types";
import { EventStatus, UserProperty, UserCustomProperty } from "@/types/events";
import splineChartMocks from "@/mocks/splineChart.json";

import eventSegmentationsMocks from "@/mocks/eventSegmentations/eventSegmentations.json";
import eventMocks from "@/mocks/eventSegmentations/events.json";

export default function ({ environment = "development" } = {}) {
    return createServer({
        routes() {
            this.namespace = "api";
            this.timing = 200;

            this.get("/schema/events", () => {
                return eventMocks
            });

            this.get("/schema/custom-events", () => {
                return [
                    {
                        id: 5,
                        createdAt: new Date(),
                        updatedAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 0,
                        isSystem: true,
                        status: EventStatus.Enabled,
                        name: "Custom event",
                        description: "This is custom event",
                        tags: ["1"]
                    }
                ];
            });

            this.get("/schema/event-properties", () => {
                return [
                    {
                        id: 1,
                        eventId: 2,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Query",
                        type: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: false,
                        dictionaryType: DataType.UInt16
                    },
                    {
                        id: 2,
                        eventId: 3,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product name",
                        type: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt16
                    },
                    {
                        id: 3,
                        eventId: 3,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product Category",
                        type: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt16
                    },
                    {
                        id: 4,
                        eventId: 3,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product Price",
                        type: DataType.Float64,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    },
                    {
                        id: 5,
                        eventId: 4,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product name",
                        type: DataType.String,
                        isArray: false,
                        nullable: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt64
                    },
                    {
                        id: 6,
                        eventId: 4,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product Category",
                        type: DataType.String,
                        isArray: false,
                        nullable: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt16
                    },
                    {
                        id: 7,
                        eventId: 4,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product Price",
                        type: DataType.Float64,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    },
                    {
                        id: 8,
                        eventId: 5,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product name",
                        type: DataType.String,
                        isArray: false,
                        nullable: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt64
                    },
                    {
                        id: 9,
                        eventId: 5,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product Category",
                        type: DataType.String,
                        isArray: false,
                        nullable: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt16
                    },
                    {
                        id: 10,
                        eventId: 5,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Product Price",
                        type: DataType.Float64,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    },
                    {
                        id: 11,
                        eventId: 5,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Discount",
                        type: DataType.Float64,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    },
                    {
                        id: 12,
                        eventId: 5,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "Revenue",
                        type: DataType.Float64,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    }
                ];
            });

            this.get("/schema/event-custom-properties", () => {
                return [
                    {
                        id: 1,
                        eventId: 1,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        tags: [],
                        name: "custom prop 1",
                        type: DataType.String,
                        isArray: false,
                        nullable: false,
                        isDictionary: false
                    }
                ];
            });

            this.get("/schema/user-properties", (): UserProperty[] => {
                return [
                    {
                        id: 1,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: "Name",
                        displayName: "Name",
                        description: "Name description",
                        status: EventStatus.Enabled,
                        type: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt64
                    },
                    {
                        id: 2,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: "Age",
                        displayName: "Age",
                        description:
                            "Lorem, ipsum dolor sit amet consectetur adipisicing elit. Architecto, temporibus.",
                        status: EventStatus.Enabled,
                        type: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt64
                    },
                    {
                        id: 3,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: "Country",
                        displayName: "Country",
                        description: "Country description",
                        status: EventStatus.Enabled,
                        type: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt64
                    },
                    {
                        id: 4,
                        createdAt: new Date(),
                        createdBy: 0,
                        updatedBy: 0,
                        projectId: 1,
                        isSystem: true,
                        tags: [],
                        name: "Device",
                        displayName: "Device",
                        description: "Device description",
                        status: EventStatus.Enabled,
                        type: DataType.String,
                        nullable: false,
                        isArray: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt64
                    }
                ];
            });

            this.get("/schema/user-custom-properties", (): UserCustomProperty[] => {
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
                        name: "Custom user prop",
                        displayName: "Custom user prop",
                        description:
                            "Lorem, ipsum dolor sit amet consectetur adipisicing elit. Architecto, temporibus.",
                        status: EventStatus.Enabled,
                        type: DataType.String,
                        isRequired: false,
                        isArray: false,
                        nullable: false,
                        isDictionary: true,
                        dictionaryType: DataType.UInt64
                    }
                ];
            });

            this.get("/data/property-values", (): string[] => {
                return ["Furniture", "Doors", "Lamp", "Tables", "Shelves"];
            });

            this.get("/chart", (): any[] => {
                return splineChartMocks;
            });

            this.post("/queries/event-segmentation", (schema, request) => {
                if (JSON.parse(request.requestBody).events.length) {
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
