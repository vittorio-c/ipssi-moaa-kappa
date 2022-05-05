import {createRouter, createWebHistory} from "vue-router";
import Maps from "@/pages/Maps";
import Graphs from "@/pages/Graphs";

const routes = [
    {
        path: "/graphs",
        name: "Graphs",
        component: Graphs,
    },
    {
        path: "/maps",
        name: "Maps",
        component: Maps,
    },
];

const router = createRouter({
    history: createWebHistory(process.env.BASE_URL),
    routes,
});

export default router;
