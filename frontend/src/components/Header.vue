<template>
  <ul
    class="h-full navbar bg-green-400 p-4 flex justify-between justify-items-center font-bold mb-4"
  >
    <li class="align-middle">
      <router-link to="/graphs">Graphs</router-link>
    </li>
    <li class="align-middle">
      <router-link to="/maps">Maps</router-link>
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