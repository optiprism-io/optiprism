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
                :delay="{ hide: 200 }"
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
            :class="{
                'pf-m-selected': isActive,
            }"
            @click="$emit('click', item)"
        >
            <span class="select-list-item__content">
                <span class="pf-c-menu__item-text">{{ text }}</span>
                <span
                    v-if="isActive"
                    class="pf-c-select__menu-item-icon"
                >
                    <UiIcon :icon="'fas fa-check'" />
                </span>
                <div
                    v-if="editable"
                    class="select-list-item__content-edit"
                    @click="edit"
                >
                    <VTooltip
                        popper-class="ui-hint"
                    >
                        <UiIcon icon="fas fa-edit" />
                        <template #popper>
                            {{ $t('common.edit') }}
                        </template>
                    </VTooltip>
                </div>
            </span>
        </div>
    </li>
</template>

<script lang="ts" setup>
// TODO add generic
import { computed } from 'vue';

const props = defineProps<{
    item: any;
    items?: any[];
    selected?: any;
    text: string;
    isDisabled?: boolean;
    editable?: boolean
    multiple?: boolean
    active?: boolean
}>();

const emit = defineEmits<{
    (e: 'click', item: any): void;
    (e: 'edit', payload: number): void
}>();

const isActive = computed(() => {
    return props.multiple && props.active
})

const isSelected = computed(() => {
    if (!props.selected || props.multiple) {
        return false;
    }

    return JSON.stringify(props.item) === JSON.stringify(props.selected);
});

const clickList = (payload: any) => {
    emit('click', {
        ...props.item,
        ...payload,
    })
}

const edit = (e: Event) => {
    e.stopPropagation()
    emit('edit', props.item.id)
}
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
        padding-right: 1rem;
        position: relative;

        &:hover {
            .select-list-item__content-edit {
                opacity: 1;
            }
        }
    }

    &__content-edit {
        position: absolute;
        top: 50%;
        right: 2px;
        transform: translateY(-50%);
        opacity: 0;
    }
}
</style>
