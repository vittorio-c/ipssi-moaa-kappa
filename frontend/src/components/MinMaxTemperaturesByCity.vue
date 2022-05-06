<template>

  <div>
    <div>
      <span class="mx-4">
        Visualiser les températures min/max par ville :
      </span>

      <select v-model="selectedOption">
        <option v-for="(opt, key) in distinctCities" v-bind:key="key">
          {{ opt }}
        </option>
      </select>
<!--      <span v-if="showSelectYear" class="mx-4">-->
<!--          Sur l'année :-->
<!--      </span>-->
<!--      <select-->
<!--        v-if="showSelectYear"-->
<!--        v-model="selectedYear"-->
<!--      >-->
<!--        <option v-for="(year, key) in distinctYears" v-bind:key="key">-->
<!--          {{ year }}-->
<!--        </option>-->
<!--      </select>-->
    </div>

    <LineChart
      v-if="chartData.datasets[0].data.length > 0"
      :chart-data="chartData"
      :height="500"
    />
  </div>
</template>
<script>
import axios from "axios";
import LineChart from "@/components/ChartLine";

export default {
  name: "MinMaxTemperaturesByCity",
  components: {LineChart},
  data() {
    return {
      temperatures: [],
      distinctCities: [],
      selectedOption: 'AALESUND, NO',
      selectedYear: 'Tout',
      currentYear: 2018
    };
  },
  computed: {
    chartData() {
      const min = this.temperatures.map((elem) => {
        return elem.min_tmp
      })
      const max = this.temperatures.map((elem) => {
        return elem.max_tmp
      })
      const labels = this.temperatures.map((elem) => {
        return elem.date
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
    // baseCity() {
    //   return this.distinctCities[0]
    // },
    url() {
      const base_url = 'http://localhost:8088/api/min-max/by-city/'

      return base_url + this.selectedOption
    },
  },
  watch: {
    url() {
      this.fetchApiData()
    }
  },
  methods: {
    fetchApiData() {
      axios
        .get(this.url)
        .then((response) => {
          this.temperatures = response.data._data;
          this.distinctCities = response.data._meta.distinct_city;
          // this.distinctYears.unshift('Tout')
        })
        .catch((error) => {
          // eslint-disable-next-line
          console.error(error);
        });
    }
  },
  created() {
    this.fetchApiData()
  },
}
</script>

