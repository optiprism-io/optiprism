import { defineStore } from "pinia";
import {
    Condition,
    PropertyRef,
} from "@/types/events";

interface Segment {
    name: string
    conditions?: Condition[]
}

type SegmentsStore = {
    segments: Segment[]
}

export const useSegmentsStore = defineStore("segments", {
    state: (): SegmentsStore => ({
        segments: [],
    }),
    getters: {},
    actions: {
        changePropertyCondition(idx: number, idxSegment: number, ref: PropertyRef) {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                const condition = segment.conditions[idx]
                if (condition) {
                    condition.propRef = ref
                }
            }
        },
        changeActionCondition(idx: number, idxSegment: number, ref: {id: string, name: string}) {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                const condition = segment.conditions[idx]
                if (condition) {
                    delete condition.propRef
                    condition.action = ref
                }
            }
        },
        removeCondition(idx: number, idxSegment: number) {
            const segment = this.segments[idxSegment]

            if (segment && segment.conditions) {
                segment.conditions.splice(idx, 1);

                if (!segment.conditions.length) {
                    delete segment.conditions
                }
            }
        },
        addConditionSegment(idx: number) {
            const segment = this.segments[idx];

            if (segment.conditions) {
                const length = segment.conditions.length - 1;
                if (segment.conditions[length] && segment.conditions[length].action) {
                    segment.conditions.push({})
                }
            } else {
                segment.conditions = [{}]
            }
        },
        renameSegment(name: string, idx: number) {
            const segment = this.segments[idx];
            if (segment) {
                segment.name = name
            }
        },
        deleteSegment(idx: number) {
            this.segments.splice(idx, 1);
        },
        addSegment(name: string) {
            this.segments.push({name})
        },
    }
});