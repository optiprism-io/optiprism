<template>
    <div class="login pf-c-background-image">
        <svg
            xmlns="http://www.w3.org/2000/svg"
            class="pf-c-background-image__filter"
            width="0"
            height="0"
        >
            <filter id="image_overlay">
                <feColorMatrix
                    type="matrix"
                    values="1 0 0 0 0 1 0 0 0 0 1 0 0 0 0 0 0 0 1 0"
                />
                <feComponentTransfer
                    color-interpolation-filters="sRGB"
                    result="duotone"
                >
                    <feFuncR
                        type="table"
                        tableValues="0.086274509803922 0.43921568627451"
                    />
                    <feFuncG
                        type="table"
                        tableValues="0.086274509803922 0.43921568627451"
                    />
                    <feFuncB
                        type="table"
                        tableValues="0.086274509803922 0.43921568627451"
                    />
                    <feFuncA
                        type="table"
                        tableValues="0 1"
                    />
                </feComponentTransfer>
            </filter>
        </svg>
    </div>
    <div class="pf-c-login">
        <div class="pf-c-login__container">
            <header class="pf-c-login__header">
                <img
                    class="pf-c-brand"
                    src="@/assets/img/logo-black.svg"
                    alt="Optyprism"
                >
            </header>
            <main class="pf-c-login__main">
                <header class="pf-c-login__main-header">
                    <h1 class="pf-c-title pf-m-3xl">
                        Log in to your account
                    </h1>
                </header>
                <div class="pf-c-login__main-body">
                    <form
                        class="pf-c-form login-form"
                        @submit.prevent="login"
                    >
                        <p class="pf-c-form__helper-text pf-m-error pf-m-hidden">
                            <span class="pf-c-form__helper-text-icon">
                                <i
                                    class="fas fa-exclamation-circle"
                                    aria-hidden="true"
                                />
                            </span>
                            Invalid login credentials.
                        </p>
                        <div class="pf-c-form__group pf-u-mb-md login-form__field">
                            <label
                                class="pf-c-form__label"
                                for="login-demo-form-username"
                            >
                                <span class="pf-c-form__label-text">
                                    Email
                                </span>
                                <span
                                    class="pf-c-form__label-required"
                                    aria-hidden="true"
                                >
                                    &#42;
                                </span>
                            </label>
                            <UiInput
                                v-model="email"
                                name="login-email"
                                :invalid="Boolean(errorFields.email)"
                            />
                            <p
                                v-if="errorFields.email"
                                class="login-form__field-info pf-c-form__helper-text pf-m-error"
                                aria-live="polite"
                            >
                                <span class="pf-c-form__helper-text-icon">
                                    <i class="fas fa-exclamation-circle" aria-hidden="true"></i>
                                </span>
                                {{ errorFields.email }}
                            </p>
                        </div>
                        <div class="pf-c-form__group pf-u-mb-md login-form__field">
                            <label
                                class="pf-c-form__label"
                                for="login-demo-form-password"
                            >
                                <span class="pf-c-form__label-text">
                                    {{ $t('login.password') }}
                                </span>
                                <span
                                    class="pf-c-form__label-required"
                                    aria-hidden="true"
                                >
                                    &#42;
                                </span>
                            </label>
                            <UiInput
                                v-model="password"
                                name="login-password"
                                type="password"
                                :invalid="Boolean(errorFields.password)"
                            />
                            <p
                                v-if="errorFields.password"
                                class="login-form__field-info pf-c-form__helper-text pf-m-error"
                                aria-live="polite"
                            >
                                <span class="pf-c-form__helper-text-icon">
                                    <i class="fas fa-exclamation-circle" aria-hidden="true"></i>
                                </span>
                                {{ errorFields.password }}
                            </p>
                        </div>
                        <div class="pf-c-form__group">
                            <div class="pf-c-check">
                                <input
                                    id="login-demo-checkbox"
                                    class="pf-c-check__input"
                                    type="checkbox"
                                    name="login-demo-checkbox"
                                >

                                <label
                                    class="pf-c-check__label"
                                    for="login-demo-checkbox"
                                >
                                    {{ $t('login.keep') }}
                                </label>
                            </div>
                        </div>
                        <div class="pf-c-form__group pf-m-action">
                            <button
                                class="pf-c-button pf-m-primary pf-m-block"
                                type="submit"
                            >
                                Log in
                            </button>
                        </div>
                    </form>
                </div>
            </main>
            <footer class="pf-c-login__footer" />
        </div>
    </div>
</template>

<script lang="ts" setup>
import { useRoute, useRouter } from 'vue-router'
import { computed, ref } from 'vue'
import { useAuthStore } from '@/stores/auth/auth'
import { pagesMap } from '@/router'
import { ErrorResponse } from '@/api'
import UiInput from '@/components/uikit/UiInput.vue'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()

const email = ref('')
const password = ref('')
const errorFields = ref<{ [key: string]: string }>({})

const nextPath = computed(() => {
    const next = route.query.next
    return next && typeof next === 'string' ? next : pagesMap.reportsEventSegmentation.path
})

const login = async (): Promise<void | Error> => {
    try {
        await authStore.login({
            email: email.value,
            password: password.value,
        })

        if (authStore.accessToken) {
            router.push({ path: nextPath.value })
        }
    } catch (e: any) {
        const errorResponce = e as ErrorResponse

        errorFields.value = errorResponce?.fields as { [key: string]: string; };
    }
}
</script>

<style lang="scss">
.pf-c-login {
    img.pf-c-brand {
        @media screen and (min-width: 1200px) {
            max-width: 400px;
        }
    }
}

.login-form {
    &__field {
        position: relative;
    }

    &__field-info {
        position: absolute;
        bottom: -1.5rem;
        left: 0;
    }
}
</style>
