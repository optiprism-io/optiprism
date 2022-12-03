import { AuthApi } from '@/api'

const api = new AuthApi()

export const authService = {
    login: (email: string, password: string) => api.basicLogin({ email, password }),
    refreshToken: (refreshToken: string) => api.authAccess({ refresh_token: refreshToken })
}
