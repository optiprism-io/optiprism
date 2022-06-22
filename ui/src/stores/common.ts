import { defineStore } from 'pinia'

export type Common = {
    showCreateCustomEvent: boolean
    showEventManagementPopup: boolean
    projectId: number
    editEventManagementPopupId: number | null
    organizationId: number
}

export const useCommonStore = defineStore('common', {
    state: (): Common => ({
        showCreateCustomEvent: false,
        showEventManagementPopup: false,
        editEventManagementPopupId: null,
        projectId: 0, // TODO integrations
        organizationId: 0,
    }),
    actions: {
        updateEditEventManagementPopupId(paylaod: number | null) {
            this.editEventManagementPopupId = paylaod
        },
        toggleEventManagementPopup(payload: boolean) {
            this.showEventManagementPopup = payload
        },
        togglePopupCreateCustomEvent(payload: boolean) {
            this.showCreateCustomEvent = payload
        },
    }
})