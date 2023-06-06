type Dictionary = { [key: string]: string }
type Placements = { [index: string]: any }
export type $T = (key: string, placements?: Placements) => string
export type $TKeyExists = (key: string) => boolean
export type I18N = {
    $t: $T,
    $tkeyExists: $TKeyExists
}

const validateKey = (key?: string) => {
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
            return res[key]
        }
        return res
    }, dictionary)
}

const i18n = {
    currentDictionaries: [] as Dictionary[],

    _setDictionary(dictionary: Dictionary = {}) {
        this.currentDictionaries.push(dictionary)
    },

    _replaceHolders(str: string, placements: Placements = {}) {
        if (Object.keys(placements).length === 0) {
            return str
        }

        return Object.keys(placements)
            .reduce((str, key) => str
                .replace(new RegExp(`:${key}`, 'g'), placements[key])
                .replace(new RegExp(`%${key}%`, 'g'), placements[key]), str)
    },

    _getTranslate(key: string, placements = {}) {
        const error = validateKey(key)
        if (error) {
            logError(error)
            return '';
        }

        for (const currentDictionary of this.currentDictionaries) {
            const dictionary = currentDictionary
            if (key in dictionary) {
                return this._replaceHolders(dictionary[key], placements)
            }

            const keys = key ? key.split('.') : []
            const res = findString(keys, dictionary)

            if (typeof res === 'string') {
                return this._replaceHolders(res, placements)
            }
        }

        logError(`$t: translate for key ${key} not found`)
        return key
    },

    _keyExists(key: string) {
        const error = validateKey(key)
        if (error) {
            logError(error)
            return false;
        }

        for (const currentDictionary of this.currentDictionaries) {
            const dictionary = currentDictionary
            if (key in dictionary) {
                return true
            }

            const keys = key.split('.')
            const res = findString(keys, dictionary)

            if (typeof res === 'string') {
                return true
            }
        }
        return false
    },

    getMethods() {
        return {
            loadDictionary: i18n._setDictionary.bind(i18n),
            t: i18n._getTranslate.bind(i18n),
            keyExists: i18n._keyExists.bind(i18n),
        }
    }
}

export default {
    ...i18n.getMethods()
}