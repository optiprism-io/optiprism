<template>
    <button
        class="pf-c-button"
        :class="{
            'pf-m-in-progress': progress,
            'pf-m-link': isLink
        }"
        :disabled="disabled"
        :type="type"
        :aria-label="ariaLabel"
    >
        <span
            v-if="beforeIcon"
            class="pf-c-button__icon pf-m-start"
        >
            <UiIcon :icon="beforeIcon" />
        </span>
        <span
            v-if="progress"
            class="pf-c-button__progress"
        >
            <UiSpinner class="pf-m-md" />
        </span>
        <UiIcon
            v-if="icon"
            :icon="icon"
        />
        <slot />
    </button>
</template>

<script setup lang="ts">
import UiSpinner from "./UiSpinner.vue";

interface Props {
    active?: boolean;
    disabled?: boolean;
    type?: "submit" | "button" | "reset" | undefined;
    progress?: boolean;
    icon?: string | undefined;
    ariaLabel?: string;
    beforeIcon?: string;
    isLink?: boolean;
}

withDefaults(defineProps<Props>(), {
    type: "button",
    beforeIcon: "",
    icon: "",
    ariaLabel: ""
});
</script>

<style lang="scss">
.pf-c-button {
    --pf-c-button--m-main--BackgroundColor: var(--pf-global--main-color--100);
    --pf-c-button--m-main--Color: var(--pf-global--Color--light-100);
    --pf-c-button--m-main--hover--BackgroundColor: var(--pf-global--main-color--200);
    --pf-c-button--m-main--hover--Color: var(--pf-global--Color--light-100);
    --pf-c-button--m-main--focus--BackgroundColor: var(--pf-global--main-color--200);
    --pf-c-button--m-main--focus--Color: var(--pf-global--Color--light-100);
    --pf-c-button--m-main--active--BackgroundColor: var(--pf-global--main-color--200);
    --pf-c-button--m-main--active--Color: var(--pf-global--Color--light-100);
    --pf-c-button--m-link--m-main--BackgroundColor: transparent;
    --pf-c-button--m-link--m-main--Color: var(--pf-global--main-color--100);
    --pf-c-button--m-link--m-main--hover--BackgroundColor: transparent;
    --pf-c-button--m-link--m-main--hover--Color: var(--pf-global--main-color--200);
    --pf-c-button--m-link--m-main--focus--BackgroundColor: transparent;
    --pf-c-button--m-link--m-main--focus--Color: var(--pf-global--main-color--200);
    --pf-c-button--m-link--m-main--active--BackgroundColor: transparent;
    --pf-c-button--m-link--m-main--active--Color: var(--pf-global--main-color--200);
    --pf-c-button--m-secondary--m-main--BackgroundColor: transparent;
    --pf-c-button--m-secondary--m-main--Color: var(--pf-global--main-color--100);
    --pf-c-button--m-secondary--m-main--after--BorderColor: var(--pf-global--main-color--100);
    --pf-c-button--m-secondary--m-main--hover--BackgroundColor: transparent;
    --pf-c-button--m-secondary--m-main--hover--Color: var(--pf-global--main-color--200);
    --pf-c-button--m-secondary--m-main--hover--after--BorderColor: var(
        --pf-global--main-color--100
    );
    --pf-c-button--m-secondary--m-main--focus--BackgroundColor: transparent;
    --pf-c-button--m-secondary--m-main--focus--Color: var(--pf-global--main-color--200);
    --pf-c-button--m-secondary--m-main--focus--after--BorderColor: var(
        --pf-global--main-color--100
    );
    --pf-c-button--m-secondary--m-main--active--BackgroundColor: transparent;
    --pf-c-button--m-secondary--m-main--active--Color: var(--pf-global--main-color--200);
    --pf-c-button--m-secondary--m-main--active--after--BorderColor: var(
        --pf-global--main-color--100
    );

    --pf-c-button--m-base-light--BackgroundColor: transparent;
    --pf-c-button--m-base-light--Color: var(--op-base-light);
    --pf-c-button--m-base-light--hover--BackgroundColor: transparent;
    --pf-c-button--m-base-light--hover--Color: var(--pf-global--Color--light-200);
    --pf-c-button--m-base-light--focus--BackgroundColor: transparent;
    --pf-c-button--m-base-light--focus--Color: var(--pf-global--Color--light-200);
    --pf-c-button--m-base-light--active--BackgroundColor: transparent;
    --pf-c-button--m-base-light--active--Color: var(--pf-global--Color--light-200);
    --pf-c-button--m-base-light--disabled--Color: var(--pf-global--disabled-color--200);
    --pf-c-button--m-base-light--disabled--BackgroundColor: transparent;

    --pf-c-button--m-bright--BackgroundColor: var(--pf-global--base-light-color--light-100);
    --pf-c-button--m-bright--Color: var(--pf-global--base-light-color--200);
    --pf-c-button--m-bright--hover--BackgroundColor: var(--pf-global--base-light-color--dark-100);
    --pf-c-button--m-bright--hover--Color: var(--pf-global--base-light-color--200);
    --pf-c-button--m-bright--focus--BackgroundColor: var(--pf-global--base-light-color--dark-100);
    --pf-c-button--m-bright--focus--Color: var(--pf-global--base-light-color--200);
    --pf-c-button--m-bright--active--BackgroundColor: var(--pf-global--base-light-color--dark-100);
    --pf-c-button--m-bright--active--Color: var(--pf-global--base-light-color--200);
    --pf-c-button--m-bright--disabled--Color: var(--pf-global--disabled-color--200);
    --pf-c-button--m-bright--disabled--BackgroundColor: transparent;

    box-sizing: border-box;

    &.pf-m-base-light {
        --pf-c-button--disabled--Color: var(--pf-c-button--m-base-light--disabled--Color);
        --pf-c-button--disabled--BackgroundColor: var(
            --pf-c-button--m-base-light--disabled--BackgroundColor
        );
        color: var(--pf-c-button--m-base-light--Color);
        background-color: var(--pf-c-button--m-base-light--BackgroundColor);

        &:hover {
            --pf-c-button--m-base-light--Color: var(--pf-c-button--m-base-light--hover--Color);
            --pf-c-button--m-base-light--BackgroundColor: var(
                --pf-c-button--m-base-light--hover--BackgroundColor
            );
        }

        &:active,
        &.pf-m-active {
            --pf-c-button--m-base-light--Color: var(--pf-c-button--m-base-light--active--Color);
            --pf-c-button--m-base-light--BackgroundColor: var(
                --pf-c-button--m-base-light--active--BackgroundColor
            );
        }

        &:focus {
            --pf-c-button--m-base-light--Color: var(--pf-c-button--m-base-light--focus--Color);
            --pf-c-button--m-base-light--BackgroundColor: var(
                --pf-c-button--m-base-light--focus--BackgroundColor
            );
        }
    }

    &.pf-m-bright {
        --pf-c-button--disabled--Color: var(--pf-c-button--m-bright--disabled--Color);
        --pf-c-button--disabled--BackgroundColor: var(
            --pf-c-button--m-bright--disabled--BackgroundColor
        );
        color: var(--pf-c-button--m-bright--Color);
        background-color: var(--pf-c-button--m-bright--BackgroundColor);

        &:hover {
            --pf-c-button--m-bright--Color: var(--pf-c-button--m-bright--hover--Color);
            --pf-c-button--m-bright--BackgroundColor: var(
                --pf-c-button--m-bright--hover--BackgroundColor
            );
        }

        &:active,
        &.pf-m-active {
            --pf-c-button--m-bright--Color: var(--pf-c-button--m-bright--active--Color);
            --pf-c-button--m-bright--BackgroundColor: var(
                --pf-c-button--m-bright--active--BackgroundColor
            );
        }

        &:focus {
            --pf-c-button--m-bright--Color: var(--pf-c-button--m-bright--focus--Color);
            --pf-c-button--m-bright--BackgroundColor: var(
                --pf-c-button--m-bright--focus--BackgroundColor
            );
        }
    }

    &.pf-m-main {
        color: var(--pf-c-button--m-main--Color);
        background-color: var(--pf-c-button--m-main--BackgroundColor);

        &:hover {
            --pf-c-button--m-main--Color: var(--pf-c-button--m-main--hover--Color);
            --pf-c-button--m-main--BackgroundColor: var(
                --pf-c-button--m-main--hover--BackgroundColor
            );
        }

        &:focus {
            --pf-c-button--m-main--Color: var(--pf-c-button--m-main--focus--Color);
            --pf-c-button--m-main--BackgroundColor: var(
                --pf-c-button--m-main--focus--BackgroundColor
            );
        }

        &:active,
        &.pf-m-active {
            --pf-c-button--m-main--Color: var(--pf-c-button--m-main--active--Color);
            --pf-c-button--m-main--BackgroundColor: var(
                --pf-c-button--m-main--active--BackgroundColor
            );
        }

        &.pf-m-secondary {
            --pf-c-button--m-main--Color: var(--pf-c-button--m-secondary--m-main--Color);
            --pf-c-button--m-main--BackgroundColor: var(
                --pf-c-button--m-secondary--m-main--BackgroundColor
            );
            --pf-c-button--after--BorderColor: var(
                --pf-c-button--m-secondary--m-main--after--BorderColor
            );

            &:hover {
                --pf-c-button--m-secondary--m-main--Color: var(
                    --pf-c-button--m-secondary--m-main--hover--Color
                );
                --pf-c-button--m-secondary--m-main--BackgroundColor: var(
                    --pf-c-button--m-secondary--m-main--hover--BackgroundColor
                );
                --pf-c-button--after--BorderColor: var(
                    --pf-c-button--m-secondary--m-main--hover--after--BorderColor
                );
            }

            &:focus {
                --pf-c-button--m-secondary--m-main--Color: var(
                    --pf-c-button--m-secondary--m-main--focus--Color
                );
                --pf-c-button--m-secondary--m-main--BackgroundColor: var(
                    --pf-c-button--m-secondary--m-main--focus--BackgroundColor
                );
                --pf-c-button--after--BorderColor: var(
                    --pf-c-button--m-secondary--m-main--focus--after--BorderColor
                );
            }

            &:active,
            &.pf-m-active {
                --pf-c-button--m-secondary--m-main--Color: var(
                    --pf-c-button--m-secondary--m-main--active--Color
                );
                --pf-c-button--m-secondary--m-main--BackgroundColor: var(
                    --pf-c-button--m-secondary--m-main--active--BackgroundColor
                );
                --pf-c-button--after--BorderColor: var(
                    --pf-c-button--m-secondary--m-main--active--after--BorderColor
                );
            }
        }

        &.pf-m-link {
            --pf-c-button--m-main--Color: var(--pf-c-button--m-link--m-main--Color);
            --pf-c-button--m-main--BackgroundColor: var(
                --pf-c-button--m-link--m-main--BackgroundColor
            );

            &:hover {
                --pf-c-button--m-link--m-main--Color: var(
                    --pf-c-button--m-link--m-main--hover--Color
                );
                --pf-c-button--m-link--m-main--BackgroundColor: var(
                    --pf-c-button--m-link--m-main--hover--BackgroundColor
                );
            }

            &:focus {
                --pf-c-button--m-link--m-main--Color: var(
                    --pf-c-button--m-link--m-main--focus--Color
                );
                --pf-c-button--m-link--m-main--BackgroundColor: var(
                    --pf-c-button--m-link--m-main--focus--BackgroundColor
                );
            }

            &:active,
            &.pf-m-active {
                --pf-c-button--m-link--m-main--Color: var(
                    --pf-c-button--m-link--m-main--active--Color
                );
                --pf-c-button--m-link--m-main--BackgroundColor: var(
                    --pf-c-button--m-link--m-main--active--BackgroundColor
                );
            }
        }
    }
}
</style>
