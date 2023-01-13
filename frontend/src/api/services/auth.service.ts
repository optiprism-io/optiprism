import {AuthApi, Configuration} from '@/api'
import {BASE_PATH} from '@/api/base';
import {config} from '@/api/services/config';

const api = new AuthApi(config);

export const authService = {
    login: (email: string, password: string) => api.basicLogin({email, password}),
    refreshToken: (refreshToken: string) => api.refreshToken({refreshToken: refreshToken})
}
