import {defineStore} from 'pinia';
import {Step} from '@/types/steps';
import {EventRef} from '@/types/events';

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

interface ExcludedEvent {
  event: EventRef;
  steps: ExcludedEventSteps;
}

type AddExcludedEventPayload = ExcludedEvent;
type EditExcludedEventPayload = {
  index: number;
  excludedEvent: Partial<ExcludedEvent>;
}

interface StepsStore {
    steps: Step[];
    size: number;
    unit: StepUnit;
    order: StepOrder;
    excludedEvents: ExcludedEvent[];
}

export const useStepsStore = defineStore('steps', {
    state: (): StepsStore => ({
        steps: [],
        size: 10,
        unit: 'hours',
        order: 'any',
        excludedEvents: []
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

        addExcludedEvent({ event, steps }: ExcludedEvent): void {
            this.excludedEvents.push({ event, steps });
        },
        editExcludedEvent({ index, excludedEvent }: EditExcludedEventPayload): void {
            const { event, steps } = excludedEvent
            if (event) {
                this.excludedEvents[index].event = event
            }
            if (steps) {
                this.excludedEvents[index].steps = steps
            }
        },
        deleteExcludedEvent(index: number): void {
            this.excludedEvents.splice(index, 1);
        }
    }
})
