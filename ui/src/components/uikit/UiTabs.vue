<template>
    <div
        class="pf-c-tabs"
        :class="{
            'pf-m-box': props.box,
        }"
    >
        <ul class="pf-c-tabs__list">
            <li
                v-for="item in props.items"
                :key="item.name"
                class="pf-c-tabs__item"
                :class="{
                    'pf-m-current': item.active,
                }"
                @click="onSelect(item.value)"
            >
                <component
                    :is="item.link ? 'router-link' : 'button'"
                    class="pf-c-tabs__link"
                    :to="item.link"
                >
                    <span
                        v-if="item.icon"
                        class="pf-c-tabs__item-icon"
                        aria-hidden="true"
                    >
                        <UiIcon
                            v-if="item.icon"
                            :icon="item.icon"
                        />
                    </span>
                    <span
                        class="pf-c-tabs__item-text"
                    >
                        {{ item.name }}
                    </span>
                </component>
            </li>
        </ul>
    </div>
</template>

<script lang="ts" setup>
type Item = {
    name: string,
    value: string,
    icon?: string,
    active?: boolean,
    link?: any,
}

interface Props {
    items: Item[],
    box?: boolean,
}

const props = withDefaults(defineProps<Props>(), {});

const emit = defineEmits<{
    (e: "on-select", payload: string): void;
}>();

const onSelect = (value: string) => {
    emit('on-select', value);
};
</script>

<style lang="scss" scoped></style>