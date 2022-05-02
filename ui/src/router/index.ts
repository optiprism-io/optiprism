import { createRouter, createWebHistory } from 'vue-router'

const routes = [
    {
        path: '/',
        name: 'main',
        component: () => import('@/pages/Index.vue'),
        children: [
            {
                path: '/',
                name: 'events_live_stream',
                component: () => import('@/pages/LiveStream.vue'),
            },
            {
                path: '/events',
                name: 'events',
                component: () => import('@/pages/Events.vue'),
            },
            {
                path: '/users',
                name: 'users',
                component: () => import('@/pages/Users.vue'),
            }
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
