import { createRouter, createWebHistory } from "vue-router"

const routes = [
    {
        path: '/',
        name: 'dashboard.index',
        component: () => import('@/pages/Dashboard.vue'),
        children: [
            {
                path: '/',
                name: 'dashboard_events_segmentation',
                component: () => import('@/pages/Index.vue'),
            },
            {
                path: '/funnels',
                name: 'dashboard_funnels',
                component: () => import('@/pages/Funnels.vue'),
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
