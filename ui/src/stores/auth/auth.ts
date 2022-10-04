import { defineStore } from 'pinia'
import axios from 'axios'
import { BasicLoginRequest, BasicLogin200Response } from '@/api'
import { authService } from '@/api/services/auth.service'
import { LocalStorageAccessor } from '@/utils/localStorageAccessor'
import { getCookie, setCookie, removeCookie } from 'typescript-cookie'

const TOKEN_KEY = 'accessToken'
const REFRESH_KEY = 'refreshToken'
const KEEP_LOGGED = 'keepLogged'
const HEADER_KEY = 'authorization'
const EXPIRES_DAYS = 30

export interface AuthState {
  accessToken: string | null
  refreshToken: LocalStorageAccessor,
}

interface LoginPayload extends BasicLoginRequest {
    keepLogged?: boolean
}

export const useAuthStore = defineStore('auth', {
    state: (): AuthState => ({
        accessToken: null,
        refreshToken: new LocalStorageAccessor(REFRESH_KEY),
    }),
    getters: {
        isAuthenticated(): boolean {
            return !!this.accessToken && !!this.refreshToken?.value
        },
    },
    actions: {
        async login(args: LoginPayload): Promise<void> {
            try {
                const res = await authService.login(args.email, args.password)
                this.setToken(res.data, args.keepLogged)
            } catch (e) {
                return Promise.reject(e)
            }
        },
        async authAccess(): Promise<void> {
            if (getCookie(TOKEN_KEY) ?? sessionStorage.getItem(TOKEN_KEY)) {
                if (!this.refreshToken.value) {
                    return
                }

                try {
                    const res = await authService.refreshToken(this.refreshToken.value)

                    await this.setToken(res.data, !!localStorage.getItem('keepLogged'))
                } catch (e) {
                    console.log(e)
                }
            }
        },
        setToken(token: BasicLogin200Response, keepLogged?: boolean): void {
            if (keepLogged && !getCookie(TOKEN_KEY)) {
                setCookie(TOKEN_KEY, token?.accessToken ?? '', {
                    expires: EXPIRES_DAYS
                })
                localStorage.setItem(KEEP_LOGGED, 'true')
            } else {
                sessionStorage.setItem(TOKEN_KEY, token?.accessToken ?? '')
            }
            axios.defaults.headers.common[HEADER_KEY] = token?.accessToken ? token.accessToken : ''

            this.accessToken = token.accessToken ?? ''
            this.refreshToken.value = token.refreshToken ?? ''
        },
        reset(): void {
            localStorage.removeItem(KEEP_LOGGED)
            sessionStorage.removeItem(TOKEN_KEY)
            removeCookie(TOKEN_KEY)
            this.accessToken = null
            this.refreshToken.value = null
        }
    }
})
