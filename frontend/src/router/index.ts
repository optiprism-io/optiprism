import {createRouter, createWebHistory} from 'vue-router'

export const pagesMap = {
    login: {
        path: '/login',
        name: 'login',
    },
    eventsLiveStream: {
        path: '/events',
        name: 'eventsLiveStream'
    },
    reportsEventSegmentation: {
        path: '/reports',
        name: 'reportsEventSegmentation',
    },
    dashboards: {
        path: '/dashboards',
        name: 'dashboards',
    },
    funnels: {
        name: 'reports_funnels'
    }
}

const routes = [
    {
        path: pagesMap.login.path,
        name: pagesMap.login.name,
        component: () => import('@/pages/auth/Login.vue'),
    },
    {
        path: '',
        component: () => import('@/AuthMiddleware.vue'),
        children: [
            {
                path: '',
                redirect: { name: pagesMap.dashboards.name }
            },
            {
                path: 'users',
                name: 'users',
                component: () => import('@/pages/users/Users.vue'),
            },
            {
                path: 'events',
                name: 'events',
                component: () => import('@/pages/events/Events.vue'),
                children: [
                    {
                        path: '',
                        name: pagesMap.eventsLiveStream.name,
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
                path: pagesMap.dashboards.path,
                name: pagesMap.dashboards.name,
                component: () => import('@/pages/Dashboards.vue'),
            },
            {
                path: 'reports',
                name: 'reports',
                component: () => import('@/pages/reports/Reports.vue'),
                children: [
                    {
                        path: ':id?',
                        name: pagesMap.reportsEventSegmentation.name,
                        component: () => import('@/pages/reports/EventSegmentation.vue'),
                    },
                    {
                        path: 'funnels/:id?',
                        name: 'reports_funnels',
                        component: () => import('@/pages/reports/Funnels.vue'),
                    }
                ]
            },
            {
                path: ':pathMatch(.*)*',
                name: 'index',
                redirect: '/'
            },
        ]
    },
]

export const router = createRouter({
    history: createWebHistory(),
    routes,
})
