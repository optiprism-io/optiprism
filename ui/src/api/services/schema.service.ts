import { get } from "../apiClient";

type PropertiesValues = {
    event_name: string;
    event_type: string;
    property_name: string;
    property_type: string;
};

const schemaService = {
    events: async () => await get("/schema/events", "", null),
    customEvents: async () => await get("/schema/custom-events", "", null),
    eventProperties: async () => await get("/schema/event-properties", "", null),
    eventCustomProperties: async () => await get("/schema/event-custom-properties", "", null),
    userProperties: async () => await get("/schema/user-properties", "", null),
    userCustomProperties: async () => await get("/schema/user-custom-properties", "", null),

    /**
     * GET propertiesValues
     * for event property you should incude event_name and event_type
     *
     * @param event_name: Event Name. Required if property has event type
     * @param event_type: Event Type. Required if property has event type
     * @param property_name: Property Name
     * @param property_type: Available values : event, eventCustom, user, userCustom
     */
    propertiesValues: async (params: PropertiesValues) =>
        await get("/data/property-values", "", params),


    // TODO event chart
    getEventChart: async () =>
        await get("/chart", "", null),
};

export default schemaService;
