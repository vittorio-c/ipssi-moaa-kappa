<template>

  <div>
    <div>
      <span class="mx-4">
        Visualiser les températures min/max :
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
    />
  </div>
</template>
<script>
import axios from "axios";
import LineChart from "@/components/ChartLine";

export default {
  name: "MinMaxTemperaturesByPeriods",
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
    url() {
      if (this.selectedOption === 'Par saisons') {
        return 'http://localhost:8088/api/min-max/by-season'
      }

      if (this.selectedOption === 'Par mois') {
        let year = ''
        if (this.selectedYear && this.selectedYear !== "Tout") {
          year = this.selectedYear
        }

        return 'http://localhost:8088/api/min-max/by-month/' + year
      }

      if (this.selectedOption === 'Par jours') {
        let year = ''
        if (this.selectedYear && this.selectedYear !== "Tout") {
          year = this.selectedYear
        }

        return 'http://localhost:8088/api/min-max/by-day/' + year
      }

      return 'http://localhost:8088/api/min-max/by-year'
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

