<template>
    <div class="pf-m-display-lg pf-u-min-width">
        <div class="pf-c-menu pf-m-plain pf-m-scrollable">
            <ul class="pf-c-menu__list">
                <li
                    v-for="item in itemsTabs"
                    :key="item.value"
                    class="pf-c-menu__item"
                    :class="{
                        'pf-c-menu__list-item--selected': item.active,
                    }"
                    @mouseover="onSelectTab(item.value)"
                >
                    <div class="pf-c-menu__item-main">
                        <span class="pf-c-menu__item-text">{{ item.name }}</span>
                    </div>
                </li>
            </ul>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed, inject } from 'vue';

const i18n = inject<any>('i18n')

const emit = defineEmits<{
    (e: 'on-select-tab', payload: string): void;
}>();

interface Props {
    activeTab: string,
    showEach?: boolean,
}

const props = defineProps<Props>();

const tabsMap = [
    'last',
    'since',
    'between',
    'each',
]

const itemsTabs = computed(() => {
    const items = props.showEach ? tabsMap : tabsMap.filter(item => item !== 'each')

    return items.map(item => {
        return {
            value: item,
            name: i18n.$t(`common.calendar.${ item}`),
            active: props.activeTab === item,
        }
    });
});

const onSelectTab = (value: string) => {
    emit('on-select-tab', value);
}
</script>

<style lang="scss">
</style>
