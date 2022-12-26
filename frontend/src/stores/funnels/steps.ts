import {defineStore} from 'pinia';
import {Step} from '@/types/steps';
import { EventRef } from '@/types/events'
import {EventFilter} from '@/stores/eventSegmentation/events';
import {
    EventType,
    FunnelQueryStepsInner,
    FunnelExcludeStepsSteps,
    FunnelEvent,
    PropertyRef,
    FunnelQueryExcludeInner,
    EventFilterByProperty,
    EventFilterByPropertyTypeEnum,
} from '@/api'

import { useEventName } from '@/helpers/useEventName'
import { useLexiconStore } from '@/stores/lexicon'

export const stepUnits = ['second', 'minute', 'hour', 'day', 'week', 'month', 'year'] as const;
export type StepUnit = typeof stepUnits[number];

export const stepOrders = ['exact', 'any'] as const;
export type StepOrder = typeof stepOrders[number];

export type ExcludedEventSteps = {
    type: 'all';
} | {
    type: 'between';
    from: number;
    to: number;
}

export type HoldingProperty = {
    id: number,
    name: string,
    type: EventFilterByPropertyTypeEnum
};

interface ExcludedEvent {
    event: EventRef;
    steps: ExcludedEventSteps;
    filters: EventFilter[];
}

type AddExcludedEventPayload = Omit<ExcludedEvent, 'filters'>;
type EditExcludedEventPayload = {
    index: number;
    excludedEvent: Partial<ExcludedEvent>;
}
type RemoveFilterForEventPayload = {
    index: number;
    filterIndex: number;
}
type EditFilterForEventPayload = {
    index: number;
    filterIndex: number;
    filter: Partial<EventFilter>;
}
type AddHoldingPropertyPayload = HoldingProperty;
type EditHoldingPropertyPayload = {
    index: number;
    property: HoldingProperty;
}
type AddEventToStepPayload = {
    index: number;
    event: EventRef;
}
type DeleteEventFormStepPayload = {
    index: number;
    eventIndex: number;
}
type EditStepEventPayload = {
    index: number;
    eventIndex: number;
    eventRef: EventRef;
}
type AddFilterToStepPayload = {
    index: number;
    eventIndex: number;
    filter: EventFilter;
}
type RemoveFilterForStepEventPayload = {
    index: number;
    eventIndex: number;
    filterIndex: number;
}
type EditFilterForStepEventPayload = {
    index: number;
    eventIndex: number;
    filterIndex: number;
    filter: Partial<EventFilter>;
}

interface StepsStore {
    steps: Step[];
    size: number;
    unit: StepUnit;
    order: StepOrder;
    excludedEvents: ExcludedEvent[];
    holdingProperties: HoldingProperty[];
    propsAvailableToHold: HoldingProperty[];
}

export const useStepsStore = defineStore('steps', {
    state: (): StepsStore => ({
        steps: [],
        size: 10,
        unit: 'hour',
        order: 'any',
        excludedEvents: [],
        holdingProperties: [],
        propsAvailableToHold: [],
    }),
    getters: {
        getSteps(): FunnelQueryStepsInner[] {
            const eventName = useEventName()
            const lexiconStore = useLexiconStore()

            return this.steps.map(item => {
                const events = item.events.map(event => {
                    return {
                        eventType: event.event.type,
                        eventId: event.event.id,
                        eventName: eventName(event.event),
                        filters: event.filters.map(filter => {
                            let property
                            if (filter.propRef) {
                                property = lexiconStore.property(filter.propRef)
                            }

                            return {
                                propertyName: property ? property.name : '',
                                propertyId: property ? property.id : filter.propRef?.id ?? 0,
                                propertyType: filter.propRef?.type ?? '',
                                type: 'property',
                                operation: filter.opId,
                                value: filter.values
                            }
                        })
                    }
                }) as FunnelEvent[]

                return {
                    events,
                    order: 'any',
                }
            })
        },
        getHoldingProperties(): PropertyRef[] {
            return this.holdingProperties.map(item => {
                return {
                    propertyType: item.type as any,
                    propertyId: item.id,
                    propertyName: item.name
                }
            })
        },
        getExcluded(): FunnelQueryExcludeInner[] {
            const eventName = useEventName()

            return this.excludedEvents.map((item): FunnelQueryExcludeInner => {
                return {
                    eventName: eventName(item.event),
                    eventType: item.event.type as EventType,
                    filters: item.filters.map(filter => {
                        return {
                            propertyType: filter.propRef?.type ?? '',
                            type: 'property',
                            operation: filter.opId,
                            value: filter.values
                        }
                    }) as EventFilterByProperty[],
                    steps: this.getSteps as FunnelExcludeStepsSteps
                }
            })
        },
    },
    actions: {
        addStep(step: Step): void {
            this.steps.push(step);
        },
        deleteStep(index: number): void {
            this.steps.splice(index, 1);
        },
        setSize(size: number): void {
            this.size = size;
        },
        setUnit(unit: StepUnit): void {
            this.unit = unit;
        },
        setOrder(order: StepOrder): void {
            this.order = order;
        },
        addExcludedEvent({ event, steps }: AddExcludedEventPayload): void {
            this.excludedEvents.push({
                event,
                steps,
                filters: []
            });
        },
        editExcludedEvent({ index, excludedEvent }: EditExcludedEventPayload): void {
            const { event, steps, filters } = excludedEvent
            if (event) {
                this.excludedEvents[index].event = event
            }
            if (steps) {
                this.excludedEvents[index].steps = steps
            }
            if (filters) {
                this.excludedEvents[index].filters = [
                    ...this.excludedEvents[index].filters,
                    ...filters
                ]
            }
        },
        removeFilterForEvent({index, filterIndex}: RemoveFilterForEventPayload): void {
            this.excludedEvents[index].filters.splice(filterIndex, 1)
        },
        editFilterForEvent({index, filterIndex, filter}: EditFilterForEventPayload): void {
            const prevFilter = this.excludedEvents[index].filters[filterIndex];
            this.excludedEvents[index].filters[filterIndex] = {
                ...prevFilter,
                ...filter
            }
        },
        deleteExcludedEvent(index: number): void {
            this.excludedEvents.splice(index, 1);
        },
        addHoldingProperty(payload: AddHoldingPropertyPayload): void {
            this.holdingProperties.push(payload);
        },
        editHoldingProperty({index, property}: EditHoldingPropertyPayload): void {
            this.holdingProperties[index] = property;
        },
        deleteHoldingProperty(index: number): void {
            this.holdingProperties.splice(index, 1);
        },
        clearHoldingProperties(): void {
            this.holdingProperties = [];
        },
        setPropsAvailableToHold(properties: HoldingProperty[]): void {
            this.propsAvailableToHold = properties;
        },
        addEventToStep({index, event}: AddEventToStepPayload): void {
            this.steps[index].events.push({
                event,
                filters: []
            })
        },
        editStepEvent({index, eventIndex, eventRef}: EditStepEventPayload): void {
            const event = this.steps[index]
            if (event) {
                event.events[eventIndex].event = eventRef;
                event.events[eventIndex].filters = [];
            }
        },
        deleteEventFromStep({index, eventIndex}: DeleteEventFormStepPayload): void {
            const step = this.steps[index];
            if (step) {
                step.events.splice(eventIndex, 1);
            }
        },
        addFilterToStep({index, eventIndex, filter}: AddFilterToStepPayload): void {
            this.steps[index].events[eventIndex].filters.push(filter);
        },
        removeFilterForStepEvent({index, eventIndex, filterIndex}: RemoveFilterForStepEventPayload): void {
            const step = this.steps[index];
            if (!step) {
                return
            }

            const event = step.events[eventIndex];
            if (!event) {
                return
            }

            event.filters.splice(filterIndex, 1);
        },
        editFilterForStepEvent({index, eventIndex, filterIndex, filter}: EditFilterForStepEventPayload): void {
            const step = this.steps[index];
            if (!step) {
                return
            }

            const event = step.events[eventIndex];
            if (!event) {
                return
            }

            event.filters[filterIndex] = {
                ...event.filters[filterIndex],
                ...filter
            }
        },
    }
})
