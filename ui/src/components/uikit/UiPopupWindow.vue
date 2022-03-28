<template>
    <div class="ui-popup-window">
        <div
            class="ui-popup-window__wrapper"
            ref="popupWrapper"
        >
            <div
                class="pf-c-modal-box"
                :class="props.size"
                aria-modal="true"
                aria-labelledby="modal-title"
                aria-describedby="modal-description"
                v-click-outside="onClickOutside"
            >
                <button
                    v-if="closable"
                    class="pf-c-button pf-m-plain"
                    type="button"
                    aria-label="Close"
                    @click="cancel('close-btn')"
                >
                    <i class="fas fa-times" aria-hidden="true"></i>
                </button>
                <header
                    v-if="props.title"
                    class="pf-c-modal-box__header"
                >
                    <h1
                        class="pf-c-modal-box__title"
                    >
                        {{ props.title }}
                    </h1>
                    <div
                        v-if="props.description"
                        class="pf-c-modal-box__description"
                    >
                        {{ props.description }}
                    </div>
                </header>
                <div class="pf-c-modal-box__body">
                    <slot></slot>
                </div>
                <footer
                    v-if="props.applyButton || props.cancelButton"
                    class="pf-c-modal-box__footer"
                >
                    <div class="pf-c-action-list">
                        <div
                            v-if="props.applyButton"
                            class="pf-c-action-list__item"
                        >
                            <UiButton
                                class="pf-m-main pf-m-primary"
                                type="button"
                                :progress="props.applyLoading"
                                @click="cancel('cancel-button')"
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
                                @click="apply"
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
import { ref, onMounted, onUnmounted } from 'vue'

import getScrollbarWidth from '@/helpers/getScrollbarWidth'

import UiButton from '@/components/uikit/UiButton.vue'

interface Props {
    title?: string
    description?: string
    closable?: boolean

    applyButton?: string
    cancelButton?: string

    applyLoading?: boolean

    saveBodyOverflow?: boolean
    noRemoveBodyOverflow?: boolean

    size?: 'pf-m-md' | 'pf-m-sm' | 'pf-m-lg' | null
}

const props = withDefaults(defineProps<Props>(), {
    size: 'pf-m-sm',
    closable: true,
})

const emit = defineEmits<{
    (e: 'cancel', target: string): void
    (e: 'apply'): void
    (e: 'open'): void
}>()


const popupWrapper = ref<HTMLDivElement>()
const initialBodyOverflow = ref('')

const onClickOutside = (e: MouseEvent) => {
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
})

</script>
<style lang="scss">
$text-color: #171B24;
$text-color-title: #171717;

.ui-popup-window {
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
        z-index: 18000;
    }
}
</style>
