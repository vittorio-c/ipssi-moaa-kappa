<template>
  <Header/>
  <div class="mx-auto w-5/6">

    <show-stations-by-year/>

  </div>

</template>

<script>
import ShowStationsByYear from "@/components/ShowStationsByYear";
import Header from "@/components/Header";

export default {
  name: "Maps",
  components: {ShowStationsByYear, Header}
}
</script>

<style scoped>

</style>