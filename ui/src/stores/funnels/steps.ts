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
        addHoldingProperty(property: HoldingProperty): void {
            this.holdingProperties.push(property);
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
    }
})
