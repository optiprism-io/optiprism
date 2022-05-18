<template>
    <div class="ui-calendar">
        <div class="ui-calendar__wrapper">
            <div class="pf-c-calendar-month">
                <table class="pf-c-calendar-month__calendar">
                    <thead
                        class="pf-c-calendar-month__days"
                        scope="col"
                    >
                        <tr class="pf-c-calendar-month__days-row">
                            <td
                                v-for="(day, index) in weekDays"
                                :key="day + index"
                                class="pf-c-calendar-month__day"
                            >
                                <span
                                    class="ui-calendar__day"
                                    aria-hidden="true"
                                >
                                    {{ day }}
                                </span>
                            </td>
                        </tr>
                    </thead>
                </table>
                <VirtualisedList
                    :viewport-height="220"
                    :get-node-height="getNodeHeight"
                    :tolerance="1"
                    :nodes="calendarList"
                    :initial-scroll-top="initialScrollTop"
                    class="ui-calendar__list"
                >
                    <template #cell="slotProps">
                        <div class="ui-calendar__list-item pf-u-pb-md">
                            <UiCalendarMonth
                                :month="slotProps.node.month"
                                :months-names="monthsNames"
                                :year="slotProps.node.year"
                                :active-dates="props.activeDates"
                                :ranged="currentRanged"
                                :dates="currentDates"
                                :week-days="weekDays"
                                :allow-future="props.allowFuture"
                                :years="yearsSelect"
                                :show-select-years="props.showSelectYears"
                                :disable-future-dates="props.disableFutureDates"
                                :first-day-of-week="props.firstDayOfWeek"
                                :from-select-only="props.fromSelectOnly"
                                @on-select="clickDate"
                                @on-mouseover="mouseoverDate"
                                @on-mouseleave="mouseleaveDate"
                                @set-year="setYear"
                            />
                        </div>
                    </template>
                </VirtualisedList>
                <div class="ui-calendar__footer pf-u-p-md">
                    <div
                        v-if="props.showBottomControls"
                        class="pf-u-display-flex pf-u-align-items-center"
                    >
                        <slot name="footer-right" />
                        <UiButton
                            class="pf-m-primary pf-u-ml-auto"
                            :disabled="props.disableApply"
                            @click="apply"
                        >
                            {{ props.buttonText }}
                        </UiButton>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed, ref, onMounted, watch, inject } from 'vue';
import { VirtualisedList } from 'vue-virtualised';

import { RangeValue, CurrentValue, Value, Ranged } from './UiCalendar'

import UiCalendarMonth from './UiCalendarMonth.vue';

const i18n = inject<any>('i18n')

interface Props {
    dates?: string[];
    activeDates?: string[];
    multiple: boolean;
    max?: number;
    count?: number;
    switcher?: boolean;
    switcherLabel?: string;
    buttonText?: string;
    buttonTextCancel?: string;
    offset?: number;
    showSelectYears?: boolean;
    sidebarControl?: boolean;
    allowFuture?: boolean;
    showBottomControls?: boolean;
    type?: string;
    selectMode?: string;
    disableFutureDates?: boolean;
    firstDayOfWeek?: number;
    disabledMultiple?: boolean;
    value: Value;
    fromSelectOnly?: boolean;
    disableApply?: boolean;
}

const weekDays = new Array(7).fill(0).map((_, i) => i18n.$t(`common.calendar.week_days.${i}`))
const monthsNames = new Array(12).fill(0).map((_, i) => i18n.$t(`common.calendar.months.${i}`))
const VIEWPORT_HEIGHT = 220;

const props = withDefaults(defineProps<Props>(), {
    count: 2,
    buttonText: 'Apply',
    buttonTextCancel: 'Cancel',
    offset: -1,
    showSelectYears: false,
    sidebarControl: false,
    allowFuture: false,
    showBottomControls: true,
    type: 'month',
    selectMode: 'range',
    disableFutureDates: true,
    firstDayOfWeek: 1,
    disabledMultiple: false,
    activeDates: () => [],
    dates: () => [],
    max: 0,
    switcher: false,
    switcherLabel: '',
    fromSelectOnly: false,
    disableApply: false,
});

const emit = defineEmits<{
    (e: 'change-visible-range', payload: RangeValue[]): void;
    (e: 'on-apply', payload: CurrentValue): void;
    (e: 'cancel'): void;
    (e: 'on-change', payload: CurrentValue): void;
    (e: 'set-multiple', payload: boolean): void;
}>();


const date = ref(new Date().getTime());
const start = ref(-1);
const currentDates = ref<string[]>([]);
const hovered = ref('');
const currentMultiple = ref(false);
const initialScrollTop = computed(() => props.count * VIEWPORT_HEIGHT + VIEWPORT_HEIGHT);

const calendarList = computed(() => {
    switch (props.type) {
        case 'year':
            return getYearCalendarList();
        default:
            return getDateCalendarList();
    }
});

const sortDates = computed(() => {
    const dates = [...currentDates.value];
    dates.sort((a, b) => {
        return new Date(a).getTime() > new Date(b).getTime() ? 1 : -1;
    });
    return dates;
});

const minDate = computed(() => sortDates.value[0] || null);
const maxDate = computed(() => sortDates.value[sortDates.value.length - 1] || null);

const currentValue = computed(() => ({
    from: currentMultiple.value ? null : minDate.value,
    to: currentMultiple.value ? null : maxDate.value,
    dates: currentMultiple.value ? [...sortDates.value] : [],
    multiple: currentMultiple.value,
    type: props.type,
    activeDates: props.activeDates || [],
    date: props.selectMode === 'single' && minDate.value ? minDate.value : null,
}));

const yearNow = computed(() => new Date().getFullYear());

const yearsSelect = computed(() => {
    let start = 2016;
    const years = [];
    while (start <= yearNow.value) {
        const value = start++;
        years.push({
            title: value,
            value: value
        });
    }
    return years;
});

const currentRanged = computed(() => {
    let ranged: Ranged = {from: '', to: ''};
    if (!currentMultiple.value && currentDates.value.length > 0) {
        const from = minDate.value && minDate.value === maxDate.value && currentDates.value.length === 1 && hovered.value ? (new Date(minDate.value).getTime() > new Date(hovered.value).getTime() ? hovered.value : minDate.value) : minDate.value;
        const to = minDate.value && minDate.value === maxDate.value && currentDates.value.length === 1 && hovered.value ? (new Date(maxDate.value).getTime() > new Date(hovered.value).getTime() ? maxDate.value : hovered.value) : maxDate.value;
        ranged = {
            from,
            to
        };
    }
    return ranged;
});

const getNodeHeight = () => VIEWPORT_HEIGHT;

const setPosition = (offset: number) => {
    const localDate = new Date(date.value);

    switch (props.type) {
        case 'year':
            localDate.setFullYear(localDate.getFullYear() + offset);
            break;
        default:
            localDate.setMonth(localDate.getMonth() + offset);
    }

    date.value = localDate.getTime();
    emit('change-visible-range', calendarList.value);
}

const setYear = (year: number) => {
    const offset = year - new Date(date.value).getFullYear();
    setPosition(offset * 12);
}

const getMonthForCalendar = (index: number, start = 0) => {
    const localDate = new Date(date.value);
    localDate.setDate(1);
    localDate.setMonth(localDate.getMonth() + index + start);
    return localDate.getMonth() + 1;
};

const getYearForCalendar = (index: number, start = 0) => {
    const localDate = new Date(date.value);

    switch (props.type) {
        case 'year':
            localDate.setFullYear(localDate.getFullYear() + index + start);
            return localDate.getFullYear();
        default:
            localDate.setMonth(localDate.getMonth() + index + start);
            return localDate.getFullYear();
    }
};

const getDateCalendarList = () => {
    const result = [];
    const months = props.count || 2;

    for (let i = 0; i < months; i++) {
        const month = getMonthForCalendar(i, start.value);
        const year = getYearForCalendar(i, start.value);

        result.push({
            i,
            id: `${i}-${month}-${year}`,
            month,
            year
        });
    }

    return result;
};

const getYearCalendarList = () => {
    const result: RangeValue[] = [];
    const years = props.count || 2;
    const startOffset = years === 1 ? 1 : 0;

    for (let i = 0; i < years; i++) {
        const year = getYearForCalendar(i, start.value + startOffset);

        result.push({
            id: `${i}-${year}`,
            year,
        });
    }
    return result;
};

const setMultiple = (e: boolean) => {
    if (!e) {
        currentDates.value = [minDate.value, maxDate.value].filter(item => item).map(item => String(item));
    } else {
        currentDates.value = [];
    }
    currentMultiple.value = e;
    emit('on-change', currentValue.value);
    emit('set-multiple', e);
};

const initValues = () => {
    start.value = props.offset;

    if (typeof props.value === 'object') {
        if (props.value.multiple) {
            currentDates.value = props.value.dates || [];
            currentMultiple.value = true;
        } else {
            const dates = [];
            if (props.value.from) {
                dates.push(props.value.from);
            }
            if (props.value.to) {
                dates.push(props.value.to);
            }
            currentDates.value = dates;
        }
    } else if (Array.isArray(props.dates)) {
        setMultiple(props.multiple || false);
        currentDates.value = props.dates;
    }
};

const cancel = () => {
    emit('cancel');
    initValues();
};

const apply = () => {
    switch (props.selectMode) {
        case 'single':
            emit('on-apply', currentValue.value);
            break;
            // TOOD other mode calendate
        default:
            emit('on-apply', currentValue.value);
    }
};


const onDateClickSingle = (e: string) => {
    currentDates.value = [e];
    emit('on-change', currentValue.value);
};

const onDateClickRange = (e: string) => {
    let dates = [];

    if (props.fromSelectOnly) {
        dates = [e, currentDates.value[1] ? currentDates.value[1] : currentDates.value[0]];
    } else if (!currentMultiple.value && Array.isArray(currentDates.value) && currentDates.value.length >= 2) {
        dates = [e];
    } else {
        dates = [...currentDates.value, e];
        if (currentMultiple.value) {
            if (currentDates.value.includes(e)) {
                dates = currentDates.value.filter(item => item !== e);
            } else if (props.max && currentDates.value.length >= props.max) {
                dates.shift();
            }
        } else {
            if (currentDates.value.length >= 2) {
                dates.shift();
            }
        }
    }
    currentDates.value = dates;
    emit('on-change', currentValue.value);
};

const clickDate = (e: string) => {
    switch (props.selectMode) {
        case 'single':
            onDateClickSingle(e);
            break;
        default:
            onDateClickRange(e);
    }
};

const mouseoverDate = (payload: string) => {
    if (props.selectMode === 'range') {
        hovered.value = payload;
    }
};

const mouseleaveDate = () => {
    hovered.value = '';
};

onMounted(() => {
    initValues();
});


watch(() => props.multiple, value => {
    currentMultiple.value = value;
});

watch(() => props.dates, value => {
    currentDates.value = value;
});

watch(() => props.value, (value) => {
    if (typeof value === 'object') {
        if (value.multiple) {
            currentDates.value = value.dates || [];
            currentMultiple.value = true;
        } else {
            const dates = [];
            if (value.from) {
                dates.push(value.from);
            }
            if (value.to) {
                dates.push(value.to);
            }
            currentDates.value = dates;
        }
    } else if (Array.isArray(props.dates)) {
        setMultiple(props.multiple || false);
        currentDates.value = props.dates;
    }
}
);
</script>

<style lang="scss" scoped>
.ui-calendar {
    &__list {
        scrollbar-width: none;

        &::-webkit-scrollbar {
            display: none;
        }
    }

    &__list-item {
        height: 100%;
    }

    &__day {
        width: .8rem;
    }

    &__head {
        padding-left: 2rem;
    }

    &__footer {
        border-top: 1px solid var(--pf-global--BackgroundColor--200);
    }

    .pf-c-calendar-month {
        padding: 6px 0 0;

        &__calendar {
            margin-left: 2.6rem;
        }

        &__day {
            width: 30px;
            text-transform: uppercase;
            font-weight: 300;
            font-size: .8rem;
        }
    }
}
</style>
