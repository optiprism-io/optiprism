<template>
    <div class="ui-calendar-month">
        <div class="ui-calendar-month__side">
            <div class="ui-calendar-month__title">
                {{ title }}
            </div>
        </div>
        <div class="ui-calendar-month__calendar">
            <table class="pf-c-calendar-month__calendar">
                <tbody class="pf-c-calendar-month__dates">
                    <tr
                        v-for="(row, index) in rows"
                        :key="index"
                        class="pf-c-calendar-month__dates-row"
                    >
                        <td
                            v-for="(cell, indexRow) in row"
                            :key="indexRow + '_' + cell.date"
                            class="pf-c-calendar-month__dates-cell"
                            :class="{
                                'pf-m-in-range': cell.ranged,
                                'pf-m-start-range': cell.from,
                                'pf-m-disabled': cell.disabled || cell.future,
                                'pf-m-end-range pf-m-selected': cell.to,
                                'pf-m-selected': cell.selected,
                                'pf-m-current': cell.now,
                            }"
                        >
                            <button
                                v-if="cell.date > 0"
                                class="pf-c-calendar-month__date"
                                type="button"
                                :disabled="cell.future || cell.disabled"
                                :class="{
                                    'pf-m-focus': cell.from,
                                }"
                                @click="clickItem(cell)"
                                @mouseover="mouseoverItem(cell.string)"
                                @mouseleave="mouseleaveItem(cell.string)"
                            >
                                {{ cell.date }}
                            </button>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</template>

<script lang="ts" setup>
import {computed} from "vue";
import {getStringDateByFormat, getYYYYMMDD} from '@/helpers/getStringDates';
import {
    DAYS_IN_WEEK,
    getMonthTable,
    getOrderedWeekdays,
} from '@/helpers/calendarHelper';

export interface Ranged {
    from: string | null;
    to: string | null;
}

interface CellDate {
    date: number;
    string: string;
    now: boolean;
    future: boolean;
    disabled: boolean;
    selected: boolean;
    ranged: boolean;
    from: boolean;
    to: boolean;
}

interface Year {
    title: number;
    value: number;
}

interface Props {
    month: number;
    year: number;
    dates: string[];
    weekDays: string[];
    monthsNames: string[];
    activeDates?: string[];
    ranged: Ranged | string[];
    years: Year[];
    showSelectYears: boolean;
    allowFuture: boolean;
    firstDayOfWeek: number;
    fromSelectOnly?: boolean,
}

const props = withDefaults(defineProps<Props>(), {
    lang: 'en',
    firstDayOfWeek: 1,
    activeDates: () => [],
    fromSelectOnly: false,
});

const emit = defineEmits<{
    (e: "on-select", payload: string): void;
    (e: "on-mouseover", payload: string): void;
    (e: "on-mouseleave", payload: string): void;
}>();


const now = computed((): Date => new Date());
const jsMonth = computed(() => currentMonth.value - 1);
const currentMonth = computed(() => props.month || now.value.getMonth() + 1);
const currentYear = computed(() => props.year || now.value.getFullYear());

const monthName = computed(() => {
    return props.monthsNames && props.monthsNames.length
        ? props.monthsNames[jsMonth.value]
        : getStringDateByFormat(`${currentYear.value}-${currentMonth.value}-01`, 'M');
});

const title = computed(() => `${monthName.value} ${currentYear.value}`);

const week = computed(() => {
    const weekDays = props.weekDays || ['M', 'T', 'W', 'T', 'F', 'S', 'S'];
    return getOrderedWeekdays(weekDays, props.firstDayOfWeek);
});

const rows = computed(() => {
    const rows = getMonthTable({year: currentYear.value, month: jsMonth.value}, props.firstDayOfWeek);

    type Row = CellDate[];
    const result: Row[] = [];

    for (let week = 0; week < rows.length; week++) {
        for (let day = 0; day < DAYS_IN_WEEK; day++) {
            const date = rows[week][day];
            const cell: CellDate = {
                date: date,
                string: getStringDate(date, currentMonth.value, currentYear.value),
                now: isNow(date, currentMonth.value, currentYear.value),
                future: isFuture(date, currentMonth.value, currentYear.value),
                disabled: isDisabled(date, currentMonth.value, currentYear.value),
                selected: isSelected(date, currentMonth.value, currentYear.value),
                ranged: isRanged(date, currentMonth.value, currentYear.value),
                from: isFrom(date, currentMonth.value, currentYear.value),
                to: isTo(date, currentMonth.value, currentYear.value),
            };
            if (result[week]) {
                result[week].push(cell);
            } else {
                result[week] = [cell];
            }
        }
    }
    return result;
});

const isNow = (day: number, month: number, year: number): boolean => {
    return day === now.value.getDate() && month === now.value.getMonth() + 1 && year === now.value.getFullYear();
}

const getStringDate = (day: number, month: number, year: number): string => {
    return [year, (month > 9 ? '' : '0') + month, (day > 9 ? '' : '0') + day].join('-');
}

const isFuture = (day: number, month: number, year: number) => {
    const date = new Date(getStringDate(day, month, year));
    const today = new Date(getYYYYMMDD(new Date));
    return date.getTime() > today.getTime();
}

const isDisabled = (day: number, month: number, year: number) => {
    const date = getStringDate(day, month, year);
    return Boolean(Array.isArray(props.activeDates) && props.activeDates.length && !props.activeDates.includes(date));
};

const isSelected = (day: number, month: number, year: number) => {
    const date = getYYYYMMDD(new Date(year, month - 1, day));
    return Array.isArray(props.dates) && props.dates.includes(getStringDate(day, month, year))
        && !Array.isArray(props.ranged)
        && props.ranged.from !== date
        && props.ranged.to !== date;
};

const isRangedObject = (day: number, month: number, year: number) => {
    if (props.ranged && !Array.isArray(props.ranged) && props.ranged.from && props.ranged.to) {
        const date = new Date(getStringDate(day, month, year));
        const from = new Date(props.ranged.from);
        const to = new Date(props.ranged.to);
        return date.getTime() >= from.getTime() && date.getTime() <= to.getTime();
    } else {
        return false;
    }
};

const isRanged = (day: number, month: number, year: number) => {
    return Array.isArray(props.ranged) && props.ranged.includes(getStringDate(day, month, year)) || isRangedObject(day, month, year);
};

const isFrom = (day: number, month: number, year: number) => {
    return !Array.isArray(props.ranged) && props.ranged.from === getYYYYMMDD(new Date(year, month - 1, day));
};

const isTo = (day: number, month: number, year: number) => {
    return !Array.isArray(props.ranged) && props.ranged.to === getYYYYMMDD(new Date(year, month - 1, day));
};

const clickItem = (cell: CellDate) => {
    if (!cell.disabled && !cell.future || (!cell.disabled && props.allowFuture)) {
        emit('on-select', cell.string);
    }
};

const mouseoverItem = (date: string) => {
    emit('on-mouseover', date);
};

const mouseleaveItem = (date: string) => {
    emit('on-mouseleave', date);
};
</script>

<style lang="scss">
.ui-calendar-month {
    display: flex;
    position: relative;
    height: 100%;

    &__side {
        width: 2.6rem;
        padding-top: .5rem;
        padding-left: .5rem;
    }

    &__calendar {
        display: flex;
        height: 100%;
        align-items: center;
    }
}

</style>
