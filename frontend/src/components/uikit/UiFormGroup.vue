<template>
    <div
        class="ui-form-group pf-c-form__group"
        :class="{
            'pf-m-action': props.action,
            'pf-u-mb-md': props.indent,
        }"
    >
        <label
            v-if="props.label"
            class="pf-c-form__label"
            :for="props.for"
        >
            <span class="pf-c-form__label-text">
                {{ props.label }}
            </span>
            <span
                v-if="props.required"
                class="pf-c-form__label-required"
                aria-hidden="true"
            >
                &#42;
            </span>
        </label>
        <slot />
        <p
            v-if="props.error"
            class="pf-c-form__helper-text pf-m-error"
            aria-live="polite"
        >
            <span class="pf-c-form__helper-text-icon">
                <i
                    class="fas fa-exclamation-circle"
                    aria-hidden="true"
                />
            </span>
            {{ props.error }}
        </p>
    </div>
</template>

<script setup lang="ts">
interface Props {
    error?: string
    label?: string
    for?: string
    required?: boolean
    action?: boolean
    indent?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    error: '',
    required: false,
    action: false,
    indent: true,
});
</script>
