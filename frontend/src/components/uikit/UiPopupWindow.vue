<template>
    <div class="ui-popup-window">
        <div
            ref="popupWrapper"
            class="ui-popup-window__wrapper"
            @mousedown="onClickOutside"
        >
            <div
                class="pf-c-modal-box ui-popup-window__box"
                :class="[
                    props.size,
                    {
                        'ui-popup-window__box_full-width': props.fullWidth
                    }
                ]"
                aria-modal="true"
                aria-labelledby="modal-title"
                aria-describedby="modal-description"
                @mousedown.stop
            >
                <button
                    v-if="closable"
                    class="pf-c-button pf-m-plain"
                    type="button"
                    aria-label="Close"
                    @click="cancel('close-btn')"
                >
                    <i
                        class="fas fa-times"
                        aria-hidden="true"
                    />
                </button>
                <header
                    v-if="props.title"
                    class="pf-c-modal-box__header pf-u-mb-md"
                >
                    <h1 class="pf-c-modal-box__title">
                        {{ props.title }}
                    </h1>
                    <div
                        v-if="props.description"
                        class="pf-c-modal-box__description"
                    >
                        {{ props.description }}
                    </div>
                </header>
                <div class="pf-c-modal-box__body pf-u-mb-md pf-u-pb-md">
                    <div
                        v-if="props.content"
                        v-html="props.content"
                    />
                    <slot v-else />
                </div>
                <footer
                    v-if="props.applyButton || props.cancelButton"
                    class="pf-c-modal-box__footer"
                >
                    <div
                        class="pf-c-action-list"
                        :class="{
                            'pf-u-justify-content-flex-end': props.actionButtonsRight
                        }"
                    >
                        <div
                            v-if="props.applyButton"
                            class="pf-c-action-list__item"
                        >
                            <UiButton
                                :class="props.applyButtonClass || 'pf-m-primary'"
                                type="button"
                                :disabled="props.applyLoading || props.applyDisabled"
                                :progress="props.applyLoading"
                                @click="apply"
                            >
                                {{ props.applyButton }}
                            </UiButton>
                        </div>
                        <div
                            v-if="props.cancelButton"
                            class="pf-c-action-list__item"
                        >
                            <UiButton
                                class=""
                                type="button"
                                @click="cancel('cancel-button')"
                            >
                                {{ props.cancelButton }}
                            </UiButton>
                        </div>
                    </div>
                </footer>
            </div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed } from 'vue'

import getScrollbarWidth from '@/helpers/getScrollbarWidth'
import UiButton from '@/components/uikit/UiButton.vue'

export interface Props {
    title?: string
    description?: string
    applyButton?: string
    applyButtonClass?: string
    cancelButton?: string
    content?: string
    closable?: boolean
    applyLoading?: boolean
    applyDisabled?: boolean
    saveBodyOverflow?: boolean
    noRemoveBodyOverflow?: boolean
    fullWidth?: boolean
    closableOverlay?: boolean
    actionButtonsRight?: boolean
    size?: 'pf-m-md' | 'pf-m-sm' | 'pf-m-lg' | null
    centered?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    title: undefined,
    description: undefined,
    applyButton: undefined,
    applyDisabled: false,
    cancelButton: undefined,
    actionButtonsRight: false,
    size: 'pf-m-sm',
    closable: true,
    fullWidth: true,
    centered: false,
    closableOverlay: true,
})

const emit = defineEmits<{
    (e: 'cancel', target: string): void
    (e: 'apply'): void
    (e: 'open'): void
}>()


const popupWrapper = ref<HTMLDivElement>()
const initialBodyOverflow = ref('')
const boxMargin = computed(() => props.centered ? 'auto' : '50px auto auto')

const onClickOutside = (e: MouseEvent) => {
    if (!props.closableOverlay) {
        return;
    }
    const popupWrapperElement = popupWrapper.value
    const clickScroll = popupWrapperElement && popupWrapperElement.clientWidth < e.x

    if (!clickScroll) {
        emit('cancel', 'outside')
    }
}

const cancel = (target: string) => {
    emit('cancel', target)
}

const apply = () => {
    emit('apply')
}

const escHandler = (e: KeyboardEvent) => {
    const ESC = 27;

    if(e.which === ESC) {
        e.stopImmediatePropagation();
        cancel('esc');
    }
}

onMounted(() => {
    if (props.saveBodyOverflow) {
        initialBodyOverflow.value = document.body.style.overflow
    }

    document.body.style.overflow = 'hidden'

    if (document.body.scrollHeight > window.innerHeight) {
        document.body.style.paddingRight = `${getScrollbarWidth()}px`
    }
    emit('open')

    document.documentElement.addEventListener('keydown', escHandler, true)
})

onUnmounted(() => {
    if (document.body.scrollHeight > window.innerHeight) {
        document.body.style.removeProperty('padding')
    }

    if (!props.noRemoveBodyOverflow) {
        document.body.style.removeProperty('overflow')
    }

    document.documentElement.removeEventListener('keydown', escHandler, true)

    if (props.saveBodyOverflow) {
        document.body.style.overflow = initialBodyOverflow.value
    }
});
</script>
<style lang="scss">
$text-color: #171B24;
$text-color-title: #171717;

.ui-popup-window {
    --margin-box: v-bind(boxMargin);

    &__wrapper {
        width: 100%;
        height: 100%;
        position: fixed;
        display: flex;
        justify-content: center;
        top: 0;
        left: 0;
        background-color: rgba(23, 23, 23, 0.7);
        overflow: auto;
        padding-top: 2rem;
        padding-bottom: 2rem;
        z-index: 1002;
    }

    &__box {
        margin: var(--margin-box);

        &_full-width {
            max-height: initial;
        }
    }
}

.popup-floating-popper {
    z-index: 1003;
}
</style>
