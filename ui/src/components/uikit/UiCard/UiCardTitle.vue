<template>
    <div class="pf-l-flex">
        <span class="pf-c-card__title pf-u-mr-0">
            <slot />
        </span>

        <div class="pf-l-flex__item">
            <slot name="extra" />
        </div>
    </div>
</template>
