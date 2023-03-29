import { AuthApi } from '@/api';
import { config } from '@/api/services/config';

const api = new AuthApi(config);

export const authService = {
    login: (email: string, password: string) => api.basicLogin({email, password}),
    refreshToken: (refreshToken: string) => api.refreshToken({refreshToken: refreshToken})
}
