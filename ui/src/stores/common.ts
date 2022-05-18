import { defineStore } from 'pinia'

export type Common = {
    showCreateCustomEvent: boolean
    projectId: number
}

export const useCommonStore = defineStore('common', {
    state: (): Common => ({
        showCreateCustomEvent: false,
        projectId: 0, // TODO integrations
    }),
    actions: {
        togglePopupCreateCustomEvent(payload: boolean) {
            this.showCreateCustomEvent = payload
        },
    }
})
