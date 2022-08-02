import { defineStore } from 'pinia'

export type Common = {
    showCreateCustomEvent: boolean
    showEventManagementPopup: boolean
    projectId: number
    editEventManagementPopupId: number | null
    organizationId: number
    showEventPropertyPopup: boolean
    editEventPropertyPopupId: number | null
}

export const useCommonStore = defineStore('common', {
    state: (): Common => ({
        showCreateCustomEvent: false,
        showEventManagementPopup: false,
        editEventManagementPopupId: null,
        showEventPropertyPopup: false,
        editEventPropertyPopupId: null,

        projectId: 0, // TODO integrations
        organizationId: 0,
    }),
    actions: {
        updateEditEventManagementPopupId(payload: number | null) {
            this.editEventManagementPopupId = payload
        },
        toggleEventManagementPopup(payload: boolean) {
            this.showEventManagementPopup = payload
        },
        togglePopupCreateCustomEvent(payload: boolean) {
            this.showCreateCustomEvent = payload
        },
    }
})
