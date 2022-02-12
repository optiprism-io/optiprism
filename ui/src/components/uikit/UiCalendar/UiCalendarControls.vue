<template>
    <div class="ui-calendar-controls pf-u-pt-md">
        <UiTabs
            class="pf-m-inset-md"
            :items="itemsTabs"
            :box="true"
            @on-select="onSelectTab"
        />
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
        <span
            v-if="props.warning"
            class="pf-u-warning-color-100 pf-u-p-sm pf-u-display-block"
        >
            {{ props.warningText }}
        </span>
    </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";
import UiTabs from "@/components/uikit/UiTabs.vue";
import UiInput from "@/components/uikit/UiInput.vue";

const emit = defineEmits<{
    (e: "on-select-tab", payload: string): void;
    (e: "on-select-last-count", payload: number): void;
    (e: "on-change-since", payload: string): void;
    (e: "on-change-between", payload: {type: 'from' | 'to', value: string}): void;
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

const tabsMap = [
    {
        name: "Last",
        value: "last",
    },
    {
        name: "Since",
        value: "since",
    },
    {
        name: "Between",
        value: "between",
    }
]

const textLastCount = computed(() => {
    return props.lastCount === 1 ? 'day' : 'days';
})

const itemsTabs = computed(() => {
    return tabsMap.map(item => {
        return {
            ...item,
            active: props.activeTab === item.value,
        }
    });
});

const onSelectTab = (value: string) => {
    emit('on-select-tab', value);
}

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
</script>

<style lang="scss">
.ui-calendar-controls {
    border-bottom: 1px solid #eee;
}
</style>
