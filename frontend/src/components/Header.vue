<template>
  <ul
    class="h-full navbar bg-blue-400 p-4 flex justify-start justify-items-center font-bold mb-4"
  >
    <li class="align-middle mx-8">
      <router-link to="/graphs">Graphs</router-link>
    </li>
    <li class="align-middle mx-8">
      <router-link to="/maps">Maps</router-link>
    </li>
    <li class="align-middle mx-8">
      <router-link to="/real-time">Temps réel</router-link>
    </li>
  </ul>
</template>

<script>
export default {
  name: "Header"
}
</script>

<style scoped>

</style>