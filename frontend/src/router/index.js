import {createRouter, createWebHistory} from "vue-router";
import Maps from "@/pages/Maps";
import Graphs from "@/pages/Graphs";
import RealTime from "@/pages/RealTime";

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
    {
        path: "/real-time",
        name: "RealTime",
        component: RealTime,
    },
];

const router = createRouter({
    history: createWebHistory(process.env.BASE_URL),
    routes,
});

export default router;
