<template>
  <div>
    <span class="mx-4">
      Témpérature moyenne par stations, rapportée à l'élévation :
    </span>

    <select
      v-model="selectedYear"
    >
      <option v-for="(year, key) in distinctYears" v-bind:key="key">
        {{ year }}
      </option>
    </select>

    <ChartScatter
      v-if="chartData.datasets[0].data.length > 0"
      :chart-data="chartData"
      :extra-chart-options="chartOptions"
      :height="500"
    />
  </div>
</template>

<script>
import axios from "axios";
import ChartScatter from "@/components/ChartScatter";

export default {
  name: "MeanTemperaturesWithElevationsByYear",
  components: {ChartScatter},
  data() {
    return {
      points: [],
      distinctYears: [],
      selectedYear: 'Tout',
      currentYear: 2018,
      chartOptions: {
        scales: {
          y: {
            title: {
              display: true,
              text: 'Elévation en mètres'
            }
          },
          x: {
            title: {
              display: true,
              text: 'Températures en °C'
            }
          }
        }
      }
    };
  },
  computed: {
    chartData() {
      const points = this.points.map((elem) => {
        return {
          x: elem.mean_tmp,
          y: elem.mean_elevation
        }
      })
      return {
        datasets: [
          {
            label: 'Tmp/Elevation',
            fill: false,
            borderColor: '#f87979',
            backgroundColor: '#f87979',
            data: points
          }
        ]
      }
    },
    url() {
      if (this.selectedYear !== 'Tout') {
        return 'http://localhost:8088/api/tmp-elevation/by-year/' + this.selectedYear
      }

      return 'http://localhost:8088/api/tmp-elevation/by-year'
    },
  },
  methods: {
    fetchApiData() {
      axios
        .get(this.url)
        .then((response) => {
          this.points = response.data._data;
          this.distinctYears = response.data._meta.distinct_years;
          this.distinctYears.unshift('Tout')
        })
        .catch((error) => {
          // eslint-disable-next-line
          console.error(error);
        });
    }
  },
  watch: {
    url() {
      this.fetchApiData()
    }
  },
  created() {
    this.fetchApiData()
  },
}
</script>
