import {Configuration} from '@/api';
import {BASE_PATH} from '@/api/base';

export const config = new Configuration({
    basePath: import.meta.env.VITE_API_BASE_PATH || BASE_PATH,
})