import { defineStore } from 'pinia';
import schemaService from '@/api/services/schema.service';
import {
    UserCustomProperty,
    Event,
    EventCustomProperty,
    UserProperty,
    customEventRef,
    eventRef,
    PropertyRef,
    EventRef,
    eventsQueries,
    EventQueryRef,
    EventsQuery,
} from '@/types/events';
import { Cohort } from '@/types';
import { aggregates } from '@/types/aggregate'
import { Group, Item } from '@/components/Select/SelectTypes';
import { useEventsStore, Events } from '@/stores/eventSegmentation/events';
import { PropertyType, CustomEvent, EventType, Property } from '@/api'
import { useCommonStore } from '@/stores/common'

type Lexicon = {
    cohorts: Cohort[];

    events: Event[];
    customEvents: CustomEvent[]
    eventsLoading: boolean;

    eventProperties: Property[];
    eventCustomProperties: EventCustomProperty[];
    eventPropertiesLoading: boolean;

    userProperties: UserProperty[];
    userCustomProperties: UserCustomProperty[];
    userPropertiesLoading: boolean;
};

export const useLexiconStore = defineStore('lexicon', {
    state: (): Lexicon => ({
        cohorts: [
            {
                id: 1,
                name: 'Active users'
            },
            {
                id: 2,
                name: 'iOS users'
            },
            {
                id: 3,
                name: 'Profitable users'
            }
        ],

        eventsLoading: false,
        events: [],
        customEvents: [],

        eventPropertiesLoading: false,
        eventProperties: [],
        eventCustomProperties: [],

        userPropertiesLoading: false,
        userProperties: [],
        userCustomProperties: [],
    }),
    actions: {
        async getEvents() {
            const commonStore = useCommonStore()
            this.eventsLoading = true

            try {
                this.events = await schemaService.events()

                const responseCustomEvents = await schemaService.customEvents(String(commonStore.projectId))
                this.customEvents = <CustomEvent[]>responseCustomEvents.data
            } catch (error) {
                throw new Error('error customEvents')
            }

            this.eventsLoading = false
        },
        async getEventProperties() {
            this.eventPropertiesLoading = true;
            try {
                this.eventProperties = await schemaService.eventProperties();
                this.eventCustomProperties = await schemaService.eventCustomProperties();
            } catch (error) {
                throw new Error('error getEventProperties');
            }
            this.eventPropertiesLoading = false;
        },
        async getUserProperties() {
            this.eventPropertiesLoading = true;
            try {
                this.userProperties = await schemaService.userProperties();
                this.userCustomProperties = await schemaService.userCustomProperties();
            } catch (error) {
                throw new Error('error getUserProperties');
            }
            this.eventPropertiesLoading = false;
        }
    },
    getters: {
        findEventById(state: Lexicon) {
            return (id: number): Event => {
                const e = state.events.find((event): boolean => Number(event.id) === Number(id))
                if (e) {
                    return e
                }
                throw new Error(`undefined event id: ${id}`)
            }
        },
        findEventByName(state: Lexicon) {
            return (name: string): Event => {
                const e = state.events.find((event): boolean => name === event.name)
                if (e) {
                    return e
                }
                throw new Error(`undefined event name: ${name}`)
            }
        },
        findCustomEventById(state: Lexicon) {
            return (id: number): CustomEvent => {
                const e = state.customEvents.find((event): boolean => Number(event.id) === Number(id))
                if (e) {
                    return e
                }
                throw new Error(`undefined custon event id: ${id}`)
            }
        },
        findCustomEventByName(state: Lexicon) {
            return (name: string): CustomEvent => {
                const e = state.customEvents.find((event): boolean => name === event.name)
                if (e) {
                    return e
                }
                throw new Error(`undefined custon event name: ${name}`)
            }
        },
        eventName() {
            return (ref: EventRef): string => {
                switch (ref.type) {
                    case EventType.Regular:
                        return this.findEventById(ref.id).name
                    case EventType.Custom:
                        return this.findCustomEventById(ref.id).name
                }
            };
        },
        findEvent() {
            return (ref: EventRef) => {
                switch (ref.type) {
                    case EventType.Regular:
                        return this.findEventById(ref.id)
                    case EventType.Custom:
                        return this.findCustomEventById(ref.id)
                }
            };
        },
        findEventProperties(state: Lexicon) {
            return (id: number): Property[] => {
                return state.eventProperties.filter((prop): boolean => prop.id === id)
            };
        },
        findEventCustomProperties(state: Lexicon) {
            return (id: number): EventCustomProperty[] => {
                return state.eventCustomProperties.filter((prop): boolean => prop.id === id)
            };
        },
        findEventPropertyByName(state: Lexicon) {
            return (name: string | number): Property => {
                const e = state.eventProperties.find((prop): boolean => prop.name === name)
                if (e) {
                    return e
                }
                throw new Error(`undefined property name: ${name}`)
            }
        },
        findEventPropertyById(state: Lexicon) {
            return (id: number): Property => {
                const e = state.eventProperties.find((prop): boolean => prop.id === id);
                if (e) {
                    return e;
                }
                throw new Error(`undefined property id: ${id}`)
            };
        },
        findEventCustomPropertyByName(state: Lexicon) {
            return (name: string): EventCustomProperty => {
                const e = state.eventCustomProperties.find((prop): boolean => prop.name === name)
                if (e) {
                    return e
                }
                throw new Error(`undefined custom property name: ${name}`)
            }
        },
        findEventCustomPropertyById(state: Lexicon) {
            return (id: number): EventCustomProperty => {
                const e = state.eventCustomProperties.find((prop): boolean => prop.id === id);
                if (e) {
                    return e;
                }
                throw new Error(`undefined custom property id: ${id}`)
            };
        },
        findUserPropertyByName(state: Lexicon) {
            return (name: string): UserProperty => {
                const e = state.userProperties.find((prop): boolean => prop.name === name)
                if (e) {
                    return e
                }
                throw new Error(`undefined user property name: ${name}`)
            }
        },
        findUserPropertyById(state: Lexicon) {
            return (id: number): UserProperty => {
                const e = state.userProperties.find((prop): boolean => prop.id === id);
                if (e) {
                    return e;
                }
                throw new Error(`undefined user property id: ${id}`)
            };
        },
        property() {
            return (ref: PropertyRef): Property | EventCustomProperty | UserProperty | UserCustomProperty => {
                switch (ref.type) {
                    case PropertyType.Event:
                        return this.findEventPropertyById(ref.id);
                    case PropertyType.Custom:
                        return this.findEventCustomPropertyById(ref.id);
                    case PropertyType.User:
                        return this.findUserPropertyById(ref.id);
                }
            };
        },
        propertyName() {
            return (ref: PropertyRef): string => {
                switch (ref.type) {
                    case PropertyType.Event:
                        return this.findEventPropertyById(ref.id).name;
                    case PropertyType.Custom:
                        return this.findEventCustomPropertyById(ref.id).name;
                    case PropertyType.User:
                        return this.findUserPropertyById(ref.id).name;
                }
            };
        },
        findCohortById(state: Lexicon) {
            return (id: number): Cohort => {
                const e = state.cohorts.find((cohort): boolean => cohort.id === id);
                if (e) {
                    return e;
                }
                throw new Error(`undefined cohort id: ${id}`)
            };
        },
        eventsList(state: Lexicon) {
            const eventsList: Group<Item<EventRef, null>[]>[] = [];

            const items: Item<EventRef, null>[] = [];

            state.customEvents.forEach((e: CustomEvent) => {
                items.push({
                    item: customEventRef(e),
                    name: e.name,
                    description: e?.description,
                    editable: true
                });
            });

            eventsList.push({
                type: 'custom',
                name: 'Custom Events',
                items,
                action: {
                    type: 'createCustomEvent',
                    icon: 'fas fa-plus-circle',
                    text: 'common.create',
                }
            })

            state.events.forEach((e: Event) => {
                const item: Item<EventRef, null> = {
                    item: eventRef(e),
                    name: e.displayName || e.name,
                    description: e?.description
                };

                const setToList = (name: string, item: Item<EventRef, null>) => {
                    const group = eventsList.find(g => g.name === name);

                    if (!group) {
                        eventsList.push({
                            name: name,
                            items: [item]
                        });
                    } else {
                        group.items.push(item);
                    }
                };

                if (Array.isArray(e.tags) && e.tags.length) {
                    e.tags.forEach((tag: string) => {
                        setToList(tag, item);
                    });
                } else {
                    setToList('Other', item);
                }
            });

            return eventsList;
        },
        eventsQueryAggregates() {
            return aggregates.map((aggregate): Item<EventQueryRef, null> => ({
                item: {
                    typeAggregate: aggregate.id,
                },
                name: aggregate.name || '',
            }));
        },
        eventsQueries(state: Lexicon) {
            const eventsStore: Events = useEventsStore();

            return eventsQueries.map((item) => {
                const query: Item<EventQueryRef, Item<EventQueryRef, null>[] | undefined> = {
                    item: {
                        type: item.type,
                        name: item.name || '',
                    },
                    name: item.grouped ? `${item.displayName} ${eventsStore.group}` : item.displayName,
                };

                if (item.hasAggregate && item.hasProperty) {
                    query.items = this.eventsQueryAggregates;
                }

                return query;
            })
        },
        findQuery() {
            return (ref: EventQueryRef | undefined): EventsQuery | undefined => {
                return ref ? eventsQueries.find((item: EventsQuery) => {
                    return item.name === ref.name;
                }) : ref;
            }
        },
        findQueryItem() {
            return (ref: EventQueryRef): Item<EventQueryRef, Item<EventQueryRef, null>[] | undefined> | undefined => {
                return this.eventsQueries.find(query => {
                    return JSON.stringify(query.item) === JSON.stringify(ref);
                });
            };
        },
    },
});
