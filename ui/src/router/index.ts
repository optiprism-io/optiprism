import { createRouter, createWebHistory } from 'vue-router'

const routes = [
    {
        path: '/',
        name: 'dashboard.index',
        component: () => import('@/pages/Dashboard.vue'),
        children: [
            {
                path: '/',
                name: 'dashboard_events_live_stream',
                component: () => import('@/pages/Index.vue'),
            },
            {
                path: '/events',
                name: 'dashboard_events',
                component: () => import('@/pages/Events.vue'),
            },
        ],
    },
    {
        path: '/:pathMatch(.*)*',
        name: 'index',
        redirect: '/'
    },
]

export const router = createRouter({
    history: createWebHistory(),
    routes,
})
