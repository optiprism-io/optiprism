import { App as Application } from 'vue'
import i18n from '@/utils/i18n'

export default {
    install: (app: Application) => {
        app.config.globalProperties.loadDictionary = i18n.loadDictionary
        app.config.globalProperties.$t = i18n.t
        app.config.globalProperties.$tkeyExists = i18n.keyExists;

        app.provide('$t', app.config.globalProperties.$t)
        app.provide('$tkeyExists', app.config.globalProperties.$tkeyExists)
        app.provide('i18n', { $t: app.config.globalProperties.$t })
    }
}