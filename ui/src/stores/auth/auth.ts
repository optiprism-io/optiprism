import {defineStore} from 'pinia';
import {LoginRequest, TokenResponse} from '@/api';
import {authService} from '@/api/services/auth.service';
import {LocalStorageAccessor} from '@/utils/localStorageAccessor';

export interface AuthState {
  accessToken: string | null;
  refreshToken: LocalStorageAccessor;
}

export const useAuthStore = defineStore('auth', {
    state: (): AuthState => ({
        accessToken: null,
        refreshToken: new LocalStorageAccessor('refreshToken'),
    }),
    getters: {
        isAuthenticated(): boolean {
            return !!this.accessToken && !!this.refreshToken;
        },
    },
    actions: {
        async login(args: LoginRequest): Promise<void> {
            try {
                const res = await authService.login(args.email, args.password);
                this.setToken(res.data);
            } catch (e) {
                console.log(e)
            }
        },
        async refreshToken(): Promise<void> {
            if (!this.refreshToken.value) {
                return
            }

            try {
                const res = await authService.refreshToken(this.refreshToken.value);
                this.setToken(res.data);
            } catch (e) {
                console.log(e)
            }
        },
        setToken(token: TokenResponse): void {
            this.accessToken = token.accessToken ?? '';
            this.refreshToken.value = token.refreshToken ?? '';
        },
        reset(): void {
            this.accessToken = null;
            this.refreshToken.value = null;
        }
    }
})
