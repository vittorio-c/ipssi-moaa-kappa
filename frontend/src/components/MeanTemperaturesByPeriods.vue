<template>

  <div>
    <div>
      <span class="mx-4">
        Visualiser la température moyenne :
      </span>

      <select v-model="selectedOption">
        <option v-for="(opt, key) in byOptions" v-bind:key="key">
          {{ opt }}
        </option>
      </select>
      <span v-if="showSelectYear" class="mx-4">
      Sur l'année :
    </span>
      <select
        v-if="showSelectYear"
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
  name: "MeanTemperaturesByPeriods",
  components: {LineChart},
  data() {
    return {
      temperatures: [],
      distinctYears: [],
      byOptions: ['Par années', 'Par mois', 'Par jours', 'Par saisons'],
      selectedOption: 'Par années',
      selectedYear: 'Tout',
      currentYear: 2018
    };
  },
  computed: {
    chartData() {
      const temperatures = this.temperatures.map((elem) => {
        return elem.mean_tmp
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
          }
        ]
      }
    },
    url() {
      if (this.selectedOption === 'Par saisons') {
        return 'http://localhost:8088/api/tmp/by-season'
      }

      if (this.selectedOption === 'Par mois') {
        let year = ''
        if (this.selectedYear && this.selectedYear !== "Tout") {
          year = this.selectedYear
        }

        return 'http://localhost:8088/api/tmp/by-month/' + year
      }

      if (this.selectedOption === 'Par jours') {
        let year = ''
        if (this.selectedYear && this.selectedYear !== "Tout") {
          year = this.selectedYear
        }

        return 'http://localhost:8088/api/tmp/by-day/' + year
      }

      return 'http://localhost:8088/api/tmp/by-year'
    },
    showSelectYear() {
      return this.selectedOption === 'Par mois' ||
        this.selectedOption === 'Par jours'
    }
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

