<template>
    <div class="pf-c-form">
        <div class="pf-c-form__group-label">
            <label
                class="pf-c-form__label"
                :for="props.for"
            >
                <span class="pf-c-form__label-text">{{ props.text }}</span>
                <span
                    v-if="props.required"
                    class="pf-c-form__label-required"
                    aria-hidden="true"
                >&#42;</span>
                <button
                    v-if="props.info"
                    class="pf-c-form__group-label-help"
                    aria-label="More info"
                >
                    <i
                        class="pficon pf-icon-help"
                        aria-hidden="true"
                    />
                </button>
                <div class="pf-c-form__group-control pf-u-mt-xs">
                    <slot />
                </div>
            </label>
        </div>
    </div>
</template>

<script lang="ts" setup>
interface Props {
    required?: boolean
    text: string
    for?: string
    info?: string
}

const props = defineProps<Props>()
</script>