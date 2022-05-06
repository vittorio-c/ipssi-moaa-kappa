<template>

  <div>
    <div>
      <span class="mx-4">
        Visualiser la température moyenne associée aux précipitations :
      </span>
      <select
        v-model="selectedYear"
      >
        <option v-for="(year, key) in distinctYears" v-bind:key="key">
          {{ year }}
        </option>
      </select>
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
  name: "MeanTemperaturesAndPrecipitationsByPeriods",
  components: {LineChart},
  data() {
    return {
      temperatures: [],
      distinctYears: [],
      // byOptions: ['Par années', 'Par mois', 'Par jours', 'Par saisons'],
      // selectedOption: 'Par années',
      selectedYear: 'Tout',
      currentYear: 2018
    };
  },
  computed: {
    chartData() {
      const temperatures = this.temperatures.map((elem) => {
        return elem.mean_tmp
      })
      const dew = this.temperatures.map((elem) => {
        return elem.mean_dew
      })
      const labels = this.temperatures.map((elem) => {
        return elem.date
      })
      return {
        labels: labels,
        datasets: [
          {
            label: 'Température moyenne',
            backgroundColor: '#55922c',
            data: temperatures
          },
          {
            label: 'Précipitation moyenne',
            backgroundColor: '#39acd1',
            data: dew
          }
        ]
      }
    },
    url() {
      let year = ''
      if (this.selectedYear && this.selectedYear !== "Tout") {
        year = this.selectedYear
      }

      return 'http://localhost:8088/api/tmp-dew/by-month/' + year
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
          this.distinctYears = response.data._meta.distinct_years;
          this.distinctYears.unshift('Tout')
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

