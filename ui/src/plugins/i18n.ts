import { App as Application } from 'vue';

export type $T = (key: string) => string

const validateKey = (key: string) => {
    if (typeof key !== 'string') {
        return `$t: only the string is supported, key - ${JSON.stringify(key)}`
    }
    return null
}

const logError = (error: string) => {
    console.warn(error)
}

const findString = (keys: any[], dictionary: any) => {
    return keys.reduce(function (res, key) {
        if (typeof res === 'object' && key in res) {
            return res[key];
        }
        return res;
    }, dictionary);
}

export default {
    install: (app: Application, options: any) => {
        const currentDictionaries: any = [options];

        app.config.globalProperties.loadDictionary = (dictionary: any) => {
            currentDictionaries.push(dictionary || {})
        }

        type Placements = { [index: string]: any }

        const replaceHolders = function (str: string, placements: Placements = {}) {
            if (Object.keys(placements).length === 0) {
                return str;
            }

            return Object.keys(placements)
                .reduce((str, key) => str
                    .replace(new RegExp(`:${key}`, 'g'), placements[key])
                    .replace(new RegExp(`%${key}%`, 'g'), placements[key]), str);
        };

        const getTranslate = function (key: string, placements = {}) {
            const error = validateKey(key);
            if (error) {
                logError(error);
                return;
            }

            for (const currentDictionary of currentDictionaries) {
                const dictionary = currentDictionary;
                if (key in dictionary) {
                    return replaceHolders(dictionary[key], placements);
                }

                const keys = key ? key.split('.') : [];
                const res = findString(keys, dictionary);

                if (typeof res === 'string') {
                    return replaceHolders(res, placements);
                }
            }

            logError(`$t: translate for key ${key} not found`);
            return key;
        };

        getTranslate.keyExists = (key: string) => {
            const error = validateKey(key);
            if (error) {
                logError(error);
                return;
            }

            for (const currentDictionary of currentDictionaries) {
                const dictionary = currentDictionary;
                if (key in dictionary) {
                    return true;
                }

                const keys = key.split('.');
                const res = findString(keys, dictionary);

                if (typeof res === 'string') {
                    return true;
                }
            }
            return false;
        };

        app.config.globalProperties.$t = getTranslate

        app.provide('i18n', {$t: getTranslate});
    },
};