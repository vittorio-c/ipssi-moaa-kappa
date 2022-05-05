<template>
  <div id="app">
    <!--
      The key allows us to routing a component from an identical parent component
      See https://github.com/vuejs/vue-router/issues/1490
    -->
    <router-view :key="$route.fullPath" />
  </div>
</template>
<script>

// import ShowStationsByYear from "@/components/ShowStationsByYear";

export default {
  components: {},
  data() {},
};
</script>
