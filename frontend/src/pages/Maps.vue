<template>
  <Header/>

  <show-stations-by-year/>
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