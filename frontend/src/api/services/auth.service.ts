import { AuthApi, Configuration } from '@/api'

const api = new AuthApi(new Configuration({ basePath: import.meta.env.VITE_API_BASE_PATH }));

export const authService = {
    login: (email: string, password: string) => api.basicLogin({ email, password }),
    refreshToken: (refreshToken: string) => api.authAccess({ refresh_token: refreshToken })
}
