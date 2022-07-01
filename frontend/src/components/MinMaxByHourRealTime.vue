<template>
  <LineChart
    v-if="records && chartData.datasets[0].data.length > 0"
    :chart-data="chartData"
    :height=500
  />
</template>

<script>
import LineChart from "@/components/ChartLine";

export default {
  name: "MinMaxByHourRealTime",
  components: {LineChart},
  props: {
    records: {
      required: true,
      type: Object,
    }
  },
  computed: {
    chartData() {
      let min = []
      Object.keys(this.records).forEach((key) => {
        min.push(this.records[key]['min_tmp'])
      })

      let max = []
      Object.keys(this.records).forEach((key) => {
        max.push(this.records[key]['max_tmp'])
      })

      let labels = []
      Object.keys(this.records).forEach((key) => {
        labels.push(this.records[key]['display_date'])
      })

      return {
        labels: labels,
        datasets: [
          {
            label: 'Température Min',
            backgroundColor: '#4edec8',
            data: min
          },
          {
            label: 'Température Max',
            backgroundColor: '#ca653c',
            data: max,
            fill: {
              target: 0,
              above: 'rgba(253,237,85,0.42)',   // Area will be red above the origin
            }
          },
        ]
      }
    },
  }
}
</script>

<style scoped>

</style>