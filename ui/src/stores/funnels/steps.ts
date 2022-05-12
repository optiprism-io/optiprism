import {defineStore} from 'pinia';
import {Step} from '@/types/steps';

export const stepUnits = ['seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years'] as const;
export type StepUnit = typeof stepUnits[number];

export const stepOrders = ['exact', 'any'] as const;
export type StepOrder = typeof stepOrders[number];

interface StepsStore {
    steps: Step[];
    size: number;
    unit: StepUnit;
    order: StepOrder;
}

export const useStepsStore = defineStore('steps', {
    state: (): StepsStore => ({
        steps: [],
        size: 10,
        unit: 'hours',
        order: 'any',
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


    }
})
