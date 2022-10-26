<template>
    <div class="pf-l-flex">
        <div class="pf-c-card__title pf-u-mr-0">
            <slot />
        </div>
        <div class="pf-l-flex__item">
            <slot name="extra" />
        </div>
        <template v-if="$slots.right">
            <div class="pf-l-flex__item pf-u-ml-auto">
                <slot name="right" />
            </div>
        </template>
    </div>
</template>
