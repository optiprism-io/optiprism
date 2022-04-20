import { defineStore } from 'pinia'

export type Common = {
    showCreateCustomEvent: boolean
}

export const useCommonStore = defineStore('common', {
    state: (): Common => ({
        showCreateCustomEvent: false
    }),
    actions: {
        togglePopupCreateCustomEvent(payload: boolean) {
            this.showCreateCustomEvent = payload
        },
    }
})