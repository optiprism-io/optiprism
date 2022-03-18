<template>
    <div class="ui-calendar-inputs">
        <div
            v-if="props.activeTab === 'last'"
            class="pf-u-p-md"
        >
            <div class="pf-u-display-flex pf-u-align-items-center">
                <span class="ws-example-flex-item">Last</span>
                <UiInput
                    class="pf-u-w-50 pf-u-mx-md"
                    :value="props.lastCount"
                    type="number"
                    :min="1"
                    :placeholder="'Enter a value'"
                    @input="onSelectLastCount"
                />
                <span class="ws-example-flex-item">{{ textLastCount }}</span>
            </div>
        </div>
        <div
            v-if="props.activeTab === 'since'"
            class="pf-u-p-md pf-u-display-flex pf-u-align-items-center pf-u-justify-content-center"
        >
            <UiInput
                class="pf-u-w-50 pf-u-mx-md"
                :class="{
                    'pf-m-warning': props.warning
                }"
                :value="props.since"
                type="text"
                placeholder="Since date"
                @input="onSelectSinceDate"
            />
        </div>
        <div
            v-if="props.activeTab === 'between'"
            class="pf-u-p-md pf-u-display-flex pf-u-align-items-center pf-u-justify-content-center"
        >
            <UiInput
                class="pf-u-w-50 pf-u-mx-md"
                :value="props.from"
                type="text"
                placeholder="From"
                @input="(e: Event) => onSelectBetween(e, 'from')"
            />
            <span>-</span>
            <UiInput
                class="pf-u-w-50 pf-u-mx-md"
                :value="props.to"
                type="text"
                placeholder="To"
                @input="(e: Event) => onSelectBetween(e, 'to')"
            />
        </div>
        <div
            v-if="props.activeTab === 'each'"
        >
            <div class="pf-c-menu pf-m-plain pf-m-scrollable">
                <ul class="pf-c-menu__list">
                    <!-- :class="{
                        'pf-c-menu__list-item--selected': item.active,
                    }" -->
                    <li
                        v-for="item in itemsEach"
                        :key="item.value"
                        class="pf-c-menu__item"

                        @click="onSelectEach(item.value)"
                    >
                        <div class="pf-c-menu__item-main">
                            <span class="pf-c-menu__item-text">{{ item.name }}</span>
                        </div>
                    </li>
                </ul>
            </div>
        </div>
        <span
            v-if="props.warning"
            class="pf-u-warning-color-100 pf-u-p-sm pf-u-display-block"
        >
            {{ props.warningText }}
        </span>
    </div>
</template>

<script lang="ts" setup>
import { computed, inject } from "vue";
import UiInput from "@/components/uikit/UiInput.vue";
import { eachMap, Each } from '@/components/uikit/UiCalendar/UiCalendarType'

const i18n = inject<any>('i18n')


const emit = defineEmits<{
    (e: "on-select-last-count", payload: number): void;
    (e: "on-change-since", payload: string): void;
    (e: "on-change-between", payload: {type: 'from' | 'to', value: string}): void;
    (e: 'on-change-each', payload: Each): void;
}>();

interface Props {
    lastCount?: number
    activeTab: string
    since: string
    warning?: boolean
    from?: string
    to?: string
    warningText?: string,
}

const props = withDefaults(defineProps<Props>(), {
    lastCount: 7,
    since: '',
    warning: false,
    warningText: '',
    from: '',
    to: '',
});

const textLastCount = computed(() => props.lastCount === 1 ? 'day' : 'days')

const itemsEach = computed(() => {
    return eachMap.map((key: Each): {value: Each, name: string } => {
        return {
            value: key,
            name: i18n.$t(`common.calendar.each_select.${ key}`),
        }
    })
})

const onSelectLastCount = (e: Event) => {
    const target = e.target as HTMLInputElement;
    emit('on-select-last-count', Number(target.value));
}

const onSelectSinceDate = (e: Event) => {
    const target = e.target as HTMLInputElement;
    emit('on-change-since', target.value);
}

const onSelectBetween = (e: Event, type: 'from' | 'to') => {
    const target = e.target as HTMLInputElement;
    emit('on-change-between', {
        type,
        value: target.value,
    });
}

const onSelectEach = (payload: Each) => emit('on-change-each', payload)
</script>

<style lang="scss">
.ui-calendar-inputs {
    border-bottom: 1px solid #eee;
}
</style>
