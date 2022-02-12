<template>
    <li
        class="select-list-item pf-c-menu__list-item"
        :class="{
            'pf-c-menu__list-item--selected': isSelected,
            'pf-c-menu__list-item--disabled': isDisabled,
        }"
    >
        <template v-if="items">
            <VMenu
                placement="right-start"
                :triggers="['hover']"
                :delay="{ hide: 0 }"
                :offset="[0, 0]"
                class="select-list-item__sub-menu"
            >
                <div class="pf-c-menu__item">
                    <div class="select-list-item__content">
                        <span class="pf-c-menu__item-text">{{ text }}</span>
                        <UiIcon
                            class="select-list-item__icon"
                            icon="fas fa-chevron-right"
                        />
                    </div>
                </div>
                <template #popper="{hide}">
                    <div class="pf-c-card pf-m-display-lg pf-u-min-width">
                        <div class="pf-c-menu pf-m-plain pf-m-scrollable">
                            <ul class="pf-c-menu__list">
                                <li
                                    v-for="itemInner in items"
                                    :key="itemInner.item.id"
                                    class="pf-c-menu__item"
                                    @click="() => {hide(); clickList(itemInner.item);}"
                                >
                                    <div class="pf-c-menu__item-main">
                                        <span class="pf-c-menu__item-text">{{ itemInner.name }}</span>
                                    </div>
                                </li>
                            </ul>
                        </div>
                    </div>
                </template>
            </VMenu>
        </template>
        <div
            v-else
            class="pf-c-menu__item"
            @click="$emit('click', item)"
        >
            <span class="select-list-item__content">
                <span class="pf-c-menu__item-text">{{ text }}</span>
            </span>
        </div>
    </li>
</template>

<script lang="ts" setup>
// TODO add generic
import { computed } from "vue";

const props = defineProps<{
    item: any;
    items?: any[];
    selected?: any;
    text: string;
    isDisabled?: boolean;
}>();

const emit = defineEmits<{
    (e: "click", item: any): void;
}>();

const isSelected = computed(() => {
    if (!props.selected) {
        return false;
    }

    return JSON.stringify(props.item) === JSON.stringify(props.selected);
});

const clickList = (payload: any) => {
    emit('click', {
        ...props.item,
        ...payload,
    })
};
</script>

<style lang="scss">
.pf-c-menu__item:hover,
.pf-c-menu__list-item--selected {
    background-color: var(--pf-c-menu__list-item--hover--BackgroundColor);
    cursor: pointer;
}
.pf-c-menu__list-item--disabled {
    background-color: var(--pf-c-menu__list-item--hover--BackgroundColor);
    opacity: .5;
    pointer-events: none;
    cursor: initial;
}

.pf-c-menu {
    &__list-item {
        cursor: pointer;
    }
}

.select-list-item {
    &__sub-menu {
        width: 100%;
        min-width: 100%;
    }

    &__icon {
        display: inline-block;
        color: var(--pf-c-menu__item--Color);
        font-size: .6rem;
        margin-left: 1rem;
    }

    &__content {
        display: flex;
        align-items: center;
    }
}
</style>
