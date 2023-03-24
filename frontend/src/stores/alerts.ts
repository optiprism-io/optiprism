import { defineStore } from 'pinia'
import { nanoid } from 'nanoid'
import { AlertTypeEnum } from '@/types'

export type AlertTypeEnum = typeof AlertTypeEnum[keyof typeof AlertTypeEnum]

export type Alert = {
    id: string,
    type: AlertTypeEnum
    text: string
    noClose?: boolean
}

export type CreateAlert = {
    id?: string,
    type?: AlertTypeEnum
    text: string
    time?: number
    noClose?: boolean
}

interface AlertsStore {
    items: Alert[]
}

export const useAlertsStore = defineStore('alerts', {
    state: (): AlertsStore => ({
        items: []
    }),
    actions: {
        createAlert(item: CreateAlert) {
            const id = item.id || nanoid()

            this.items.push({
                id,
                type: item.type || 'default',
                text: item.text || '',
            });

            if (item.time) {
                setTimeout(() => {
                    this.closeAlert(id)
                }, item.time)
            }
        },
        closeAlert(id: string) {
            this.items = [...this.items].filter(item => item.id !== id)
        },
    }
})
