<template>
    <div class="pf-u-display-flex pf-u-justify-content-center pf-u-align-items-center pf-u-my-2xl">
        <div>
            <div class="pf-u-m-auto pf-u-w-25 pf-u-color-400 pf-u-text-align-center">
                <UiIcon
                    class="pf-u-font-size-4xl"
                    :icon="'fas fa-search'"
                />
            </div>
            <div class="pf-c-card__title pf-u-text-align-center pf-u-font-size-lg pf-u-color-400 pf-u-mt-xl">
                <slot />
            </div>
        </div>
    </div>
</template>
