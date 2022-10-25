<template>
    <VMenu
        placement="right-start"
        :triggers="['hover']"
        :delay="{ hide: 200 }"
        :offset="[0, 0]"
        class="select-list-item__sub-menu"
    >
        <div class="pf-c-menu__item">
            <div class="select-list-item__content">
                <span class="pf-c-menu__item-text">{{ label }}</span>
                <UiIcon
                    class="select-list-item__icon"
                    icon="fas fa-chevron-right"
                />
            </div>
        </div>
        <template #popper>
            <div class="pf-c-card pf-m-display-lg pf-u-min-width">
                <div class="pf-c-menu pf-m-plain pf-m-scrollable">
                    <ul class="pf-c-menu__list">
                        <slot />
                    </ul>
                </div>
            </div>
        </template>
    </VMenu>
</template>

<script setup lang="ts">
defineProps({
    label: {
        type: String,
        default: '',
    },
})
</script>
