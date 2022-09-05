import {createRouter, createWebHistory} from 'vue-router'

const routes = [
    {
        path: '/',
        name: 'main',
        component: () => import('@/pages/Index.vue'),
        redirect: () => {
            return { name: 'events_live_stream' }
        },
    },
    {
        path: '/users',
        name: 'users',
        component: () => import('@/pages/users/Users.vue'),
    },
    {
        path: '/events',
        name: 'events',
        component: () => import('@/pages/events/Events.vue'),
        children: [
            {
                path: '',
                name: 'events_live_stream',
                component: () => import('@/pages/events/LiveStream.vue'),
            },
            {
                path: 'event_management',
                name: 'events_event_management',
                component: () => import('@/pages/events/EventManagement.vue'),
            },
            {
                path: 'custom_events',
                name: 'events_custom_events',
                component: () => import('@/pages/events/CustomEvents.vue'),
            },
            {
                path: 'event_properties',
                name: 'events_event_properties',
                component: () => import('@/pages/events/EventProperties.vue'),
            },

        ]
    },
    {
        path: '/reports',
        name: 'reports',
        component: () => import('@/pages/reports/Reports.vue'),
        children: [
            {
                path: '',
                name: 'reports_event_segmentation',
                component: () => import('@/pages/reports/EventSegmentation.vue'),
            },
            {
                path: 'funnels',
                name: 'reports_funnels',
                component: () => import('@/pages/reports/Funnels.vue'),
            }
        ]
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
