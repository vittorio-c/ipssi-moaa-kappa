<template>
  <Scatter
    :chart-options="chartOptions"
    :chart-data="chartData"
    :chart-id="chartId"
    :dataset-id-key="datasetIdKey"
    :plugins="plugins"
    :css-classes="cssClasses"
    :styles="styles"
    :width="width"
    :height="height"
  />
</template>

<script>
import {Scatter} from 'vue-chartjs'

import {
  Chart as ChartJS,
  Title,
  Tooltip,
  Legend,
  LineElement,
  CategoryScale,
  PointElement,
  LinearScale
} from 'chart.js'

ChartJS.register(
  Title,
  Tooltip,
  Legend,
  LineElement,
  CategoryScale,
  PointElement,
  LinearScale
)

export default {
  name: 'ChartScatter',
  components: {
    Scatter
  },
  computed: {
    chartOptions() {
      return {...this.baseChartOptions, ...this.extraChartOptions};
    }
  },
  props: {
    chartData: {
      type: Object,
      default: () => {}
    },
    chartId: {
      type: String,
      default: 'scatter-chart'
    },
    datasetIdKey: {
      type: String,
      default: 'label'
    },
    width: {
      type: Number,
      default: 400
    },
    height: {
      type: Number,
      default: 400
    },
    cssClasses: {
      default: '',
      type: String
    },
    styles: {
      type: Object,
      default: () => {
      }
    },
    plugins: {
      type: Array,
      default: () => []
    },
    extraChartOptions: {
      type: Object,
      default: () => {}
    }
  },
  data() {
    return {
      baseChartOptions: {
        responsive: true,
        maintainAspectRatio: false,
      }
    }
  }
}
</script>
