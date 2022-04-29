<template>
    <VDropdown
        placement="bottom-start"
        :triggers="[]"
        popper-class="ui-date-picker"
        :shown="isOpen"
        @hide="onHide"
    >
        <template v-if="$slots.action">
            <div @click="onToggle">
                <slot name="action" />
            </div>
        </template>
        <template #popper="{hide}">
            <div class="ui-date-picker__content">
                <div class="ui-date-picker__tabs">
                    <UiCalendarControls
                        v-if="props.showControls"
                        :active-tab="activeTab"
                        :show-each="showEach"
                        @on-select-tab="onSelectTab"
                    />
                </div>
                <div class="ui-date-picker__action">
                    <UiCalendarInputs
                        :active-tab="activeTab"
                        :since="since"
                        :last-count="lastCountLocal"
                        :warning="warning"
                        :warning-text="warningText"
                        :from="betweenValue.from"
                        :to="betweenValue.to"
                        :each="value.each"
                        @on-select-last-count="onSelectLastCount"
                        @on-change-since="onChangeSince"
                        @on-change-between="onChangeBetween"
                        @on-change-each="onChangeEach"
                    />
                    <UiCalendar
                        v-if="showCalendar"
                        :multiple="true"
                        :value="valueLocal"
                        :count="props.monthLength"
                        :offset="props.offsetMonth"
                        :from-select-only="fromSelectOnly"
                        :disable-apply="warning"
                        :button-text="$t('common.apply')"
                        @on-change="onChange"
                        @on-apply="($event: any) => {hide(); apply($event)}"
                    />
                </div>
            </div>
        </template>
    </VDropdown>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from 'vue';
import { getYYYYMMDD } from '@/helpers/getStringDates';
import { getLastNDaysRange, dateDiff, isDate } from '@/helpers/calendarHelper';

import UiCalendarControls from '@/components/uikit/UiCalendar/UiCalendarControls.vue';
import UiCalendarInputs from './UiCalendar/UiCalendarInputs.vue'
import UiCalendar from '@/components/uikit/UiCalendar/UiCalendar.vue';

import { Each, ApplyPayload, CurrentValue, Value } from '@/components/uikit/UiCalendar/UiCalendar'

interface Props {
    showControls?: boolean
    value: Value
    lastCount?: number
    activeTabControls?: string
    offsetMonth?: number
    monthLength?: number
    showEach?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    showControls: true,
    activeTabControls: 'last',
    lastCount: 7,
    offsetMonth: -24,
    monthLength: 25,
});

const emit = defineEmits<{
    (e: 'on-select', payload: string): void;
    (e: 'on-apply', payload: ApplyPayload): void;
    (e: 'on-change-each', payload: Each): void;
}>();

const activeTab = ref('last');
const since = ref('');
const isOpen = ref(false);
const lastCountLocal = ref(7);
const valueLocal = ref({
    from: '',
    to: '',
    multiple: false,
})
const betweenValue = ref({
    from: '',
    to: '',
})
const warning = ref(false);
const warningText = ref('');

const fromSelectOnly = computed(() => {
    const isOneDateSelectTabs = ['last', 'since'];

    return props.showControls && isOneDateSelectTabs.includes(activeTab.value);
});

const firsDateCalendar = computed((): Date => {
    const firsDateCalendar = new Date();
    firsDateCalendar.setMonth(firsDateCalendar.getMonth() - props.monthLength);

    return firsDateCalendar;
});

const showCalendar = computed(() => activeTab.value !== 'each')

const onToggle = () => {
    isOpen.value = !isOpen.value;
};

const onHide = () => {
    isOpen.value = false
}

const onSelectTab = (type: string) => {
    if (type === 'last') {
        lastCountLocal.value = dateDiff(valueLocal.value.from, getYYYYMMDD(new Date())) + 1;
    }
    activeTab.value = type;
    since.value = '';
    warning.value = false;
    warningText.value = '';
    if (lastCountLocal.value < 1) {
        lastCountLocal.value = 1;
    }
    updateValue();
}

const resetBetween = () => {
    const lastNDateRange = getLastNDaysRange(lastCountLocal.value);
    return {
        from: getYYYYMMDD(lastCountLocal.value === 0 ? new Date() : lastNDateRange.from),
        to: getYYYYMMDD(new Date()),
    };
}

const onSelectLastCount = (payload: number) => {
    lastCountLocal.value = payload;
    warning.value = payload === 0;

    valueLocal.value = {
        ...valueLocal.value,
        ...resetBetween(),
    }
}

const onChangeSince = (payload: string) => {
    since.value = payload;
    warning.value = false;
    warningText.value = '';

    if (isDate(payload)) {
        const newDate = new Date(payload);
        const newDateTimestamp = newDate.getTime();

        if (firsDateCalendar.value.getTime() < newDateTimestamp && new Date().getTime() > newDateTimestamp) {
            valueLocal.value = {
                ...valueLocal.value,
                from: getYYYYMMDD(newDate),
            }
            lastCountLocal.value = dateDiff(valueLocal.value.from, getYYYYMMDD(new Date())) + 1;

        } else {
            warning.value = true;
            warningText.value = 'The selected date is greater or less than the allowed dates';
        }
    } else {
        valueLocal.value = {
            ...valueLocal.value,
            ...resetBetween(),
        }
    }
}

const onChangeBetween = (payload: {type: 'from' | 'to', value: string}) => {
    betweenValue.value = {
        ...betweenValue.value,
        [payload.type]: payload.value,
    }
    warning.value = false;
    warningText.value = '';

    if (isDate(payload.value)) {
        const newDate = new Date(payload.value);
        const newDateTimestamp = newDate.getTime();

        if (firsDateCalendar.value.getTime() < newDateTimestamp && new Date().getTime() > newDateTimestamp) {
            valueLocal.value = {
                ...valueLocal.value,
                [payload.type]: getYYYYMMDD(newDate),
            }

            if (new Date(valueLocal.value.from).getTime() > new Date(valueLocal.value.to).getTime()) {
                warning.value = true;
            }
        } else {
            warning.value = true;
            warningText.value = 'The selected date is greater or less than the allowed dates';
        }
    } else {
        valueLocal.value = {
            ...valueLocal.value,
            ...resetBetween(),
        }
    }
}

const onChangeEach = (payload: Each) => {
    isOpen.value = false
    emit('on-change-each', payload)
}

const updateValue = () => {
    const value = {...props.value};

    if (!value.from && !value.to) {
        const reset = resetBetween();
        value.from = reset.from;
        value.to = reset.to;
    }

    since.value = value.from;
    betweenValue.value = {
        from: value.from,
        to: value.to,
    };
    valueLocal.value = value;
}

const onChange = (payload: CurrentValue): void => {
    const value = {
        from: payload.from || '',
        to: payload.to || '',
    };

    since.value = value.from;
    betweenValue.value = value;
    lastCountLocal.value = dateDiff(value.from, getYYYYMMDD(new Date())) + 1;
}

const apply = (payload: CurrentValue): void => {
    emit('on-apply', {
        value: payload,
        type: activeTab.value,
        last: Number(lastCountLocal.value),
    });
}

onMounted(() => {
    if (props.activeTabControls) {
        activeTab.value = props.activeTabControls;
    }

    if (props.lastCount) {
        lastCountLocal.value = props.lastCount;
    }

    updateValue();
});

watch(() => props.lastCount, (value) => {
    lastCountLocal.value = value;
    updateValue();
})
</script>

<style lang="scss">
.ui-date-picker {
    &__content {
        display: flex;
        overflow: hidden;
    }

    &__tabs {
        width: 200px;
    }

    &__action {
        width: 280px;
        border-left: 1px solid var(--pf-global--BackgroundColor--200);
    }

    .pf-c-menu.pf-m-plain {
        box-shadow: none;
    }
}
</style>