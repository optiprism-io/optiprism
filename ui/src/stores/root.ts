import { defineStore } from 'pinia'

interface Root {
    projectId: number
}

export const useRootStore = defineStore('root', {
    state: (): Root => ({
        projectId: 0, // TODO integrations
    }),
})