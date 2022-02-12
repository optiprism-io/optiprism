const DEFAULT_FIRST_DAY = 1;
const MAX_WEEKS_COUNT = 6;
export const DAYS_IN_WEEK = 7;

const getDateShiftByFirstWeekDay = (firstDayOfWeek: number) => firstDayOfWeek === 1 ? 1 : 0;
const getFirstDayOfWeek = (firstDayBackend: number = DEFAULT_FIRST_DAY): number => {
    switch (firstDayBackend) {
        case 1:
            return 1;
        case 7:
            return 0;
        default:
            return 1;
    }
};

export function isDate(date: string) {
    return !isNaN(Date.parse(date));
}

export function dateDiff(first: string, second: string) {
    return Math.round((new Date(second).getTime() - new Date(first).getTime()) / (1000 * 60 * 60 * 24));
}

export function getDaysInMonthsCount({year, month}: { year: number, month: number }) {
    return 32 - (new Date(year, month, 32)).getDate();
}

export function getOrderedWeekdays(days: string[], firstDayBackend = DEFAULT_FIRST_DAY) {
    return getFirstDayOfWeek(firstDayBackend) === 0
        ? days.slice(-1).concat(days.slice(0, -1))
        : days;
}

export function getMonthTable({year, month}: { year: number, month: number }, firstDayBackend = DEFAULT_FIRST_DAY) {
    const firstDay = (new Date(year, month, 1)).getDay() - getDateShiftByFirstWeekDay(getFirstDayOfWeek(firstDayBackend));

    type Week = number[];
    const result: Week[] = [];
    const daysInMonth = getDaysInMonthsCount({year, month});
    let isOneDayWeek = false;

    for (let week = 0; week < MAX_WEEKS_COUNT; week++) {
        result.push([]);
        for (let day = 0; day < DAYS_IN_WEEK; day++) {
            const date = week * DAYS_IN_WEEK + day - firstDay + 1;
            if (week === 0 && day === 0 && date === 2 && firstDay === -1) {
                isOneDayWeek = true;
            }

            const weekDay = week === 0
                ? (day >= firstDay ? date : 0)
                : (date > daysInMonth ? 0 : date);

            result[week].push(weekDay);
        }
    }

    if (isOneDayWeek) {
        const firstWeek = [];
        firstWeek[6] = 1;
        result.unshift(firstWeek);
    }

    return result.filter(row => row.some(cell => cell));
}

export function getDateByDaysOffset(offset: number, startDate = new Date()) {
    return new Date((new Date(startDate)).setDate(startDate.getDate() + offset));
}

export function getYesterday() {
    const yesterday = getDateByDaysOffset(-1);
    return {
        from: yesterday,
        to: yesterday
    };
}

export function getLastNDaysRange(daysCount: number) {
    const now = new Date();

    const to = new Date(now);
    const from = getDateByDaysOffset(-(daysCount - 1));

    return {from, to};
}

export function getLastNMonthsRange(monthsCount = 1) {
    const now = new Date();
    const currMonth = now.getMonth();
    let offset = 0;
    for (let i = 0; i < monthsCount; i++) {
        offset += getDaysInMonthsCount({year: now.getFullYear(), month: currMonth - 1 - i});
    }
    return getLastNDaysRange(offset);
}

export function getWeekStartDate(firstDayBackend = DEFAULT_FIRST_DAY, startDate = new Date()) {
    const daysFromWeekStart = startDate.getDay() - getDateShiftByFirstWeekDay(getFirstDayOfWeek(firstDayBackend));
    return getDateByDaysOffset(-daysFromWeekStart, startDate);
}

export function getWeekEndDate(firstDayBackend = DEFAULT_FIRST_DAY, endDate = new Date()) {
    const firstDayOfWeek = getFirstDayOfWeek(firstDayBackend);
    const day = endDate.getDay();
    let offset;
    if (!day && firstDayOfWeek == 1) {
        offset = 0;
    } else {
        offset = 6 - day + getDateShiftByFirstWeekDay(getFirstDayOfWeek(firstDayBackend));
    }
    return getDateByDaysOffset(
        offset,
        endDate
    );
}

export function getCurrentWeekRange(firstDayBackend = DEFAULT_FIRST_DAY) {
    const now = new Date();

    const to = now;
    const from = getDateByDaysOffset(-(now.getDay() - getDateShiftByFirstWeekDay(getFirstDayOfWeek(firstDayBackend))));

    return {from, to};
}

export function getPreviousWeekRange(firstDayBackend = DEFAULT_FIRST_DAY) {
    const now = new Date();

    const to = getDateByDaysOffset(-(now.getDay() + 1 - getDateShiftByFirstWeekDay(getFirstDayOfWeek(firstDayBackend))));
    const from = getDateByDaysOffset(-6, to);

    return {from, to};
}

export function getCurrentMonthRange() {
    const from = new Date((new Date()).setDate(1));
    const to = new Date((new Date(from.getFullYear(), from.getMonth() + 1)).setDate(0));

    return {from, to};
}

export function getLastMonthRange() {
    const now = new Date();

    const to = new Date((new Date(now.getFullYear(), now.getMonth())).setDate(0));
    const from = new Date((new Date(to)).setDate(1));

    return {from, to};
}

export function getYearRange() {
    const now = new Date();

    const from = new Date((new Date(now)).setFullYear(now.getFullYear() - 1));
    const to = new Date(now);

    return {from, to};
}