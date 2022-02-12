import { createRouter, createWebHistory } from "vue-router"

import Index from "@/pages/Index.vue";
import Dashboard from "@/pages/Dashboard.vue";
import Funnels from "@/pages/Funnels.vue";

const routes = [
    {
        path: '/',
        name: 'dashboard.index',
        component: Dashboard,
        children: [
            {
                path: '/',
                name: 'dashboard_events_segmentation',
                component: Index,
            },
            {
                path: '/funnels',
                name: 'dashboard_funnels',
                component: Funnels,
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
