<template>
  <Header/>
  <mean-temperatures-by-periods/>
</template>

<script>
import Header from "@/components/Header";
import MeanTemperaturesByPeriods from "@/components/MeanTemperaturesByPeriods";

export default {
  name: "Maps",
  components: {MeanTemperaturesByPeriods, Header}
}
</script>

<style scoped>

</style>