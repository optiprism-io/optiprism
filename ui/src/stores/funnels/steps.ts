import {defineStore} from 'pinia';
import {Step} from '@/types/steps';
import {EventProperty, EventRef} from '@/types/events';
import {EventFilter} from '@/stores/eventSegmentation/events';

export const stepUnits = ['seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years'] as const;
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

export type HoldingProperty = Pick<EventProperty, 'id' | 'name'>;

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
        unit: 'hours',
        order: 'any',
        excludedEvents: [],
        holdingProperties: [],
        propsAvailableToHold: [],
    }),
    getters: {},
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
        deleteEventFromStep(index: number): void {
            this.steps[index].events.splice(index, 1);
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
