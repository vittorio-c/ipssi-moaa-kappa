<template>
  <Header/>
  <div class="mx-auto w-5/6">
    <ul class="flex flex-wrap mb-6">
      <li class="mr-2">
        <button
          @click="activateTab(1)"
          class="inline-block p-4 rounded-t-lg border-b-2"
          :class="(isActiveTab(1)) ?
            'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' :
            'border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300'"
        >
          Températures moyennes
        </button>
      </li>
      <li>
        <button
          @click="activateTab(2)"
          class="inline-block p-4 rounded-t-lg border-b-2"
          :class="(isActiveTab(2)) ?
            'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' :
            'border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300'"
        >
          Températures min/max
        </button>
      </li>
      <li>
        <button
          @click="activateTab(3)"
          class="inline-block p-4 rounded-t-lg border-b-2"
          :class="(isActiveTab(3)) ?
            'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' :
            'border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300'"
        >
          Températures moyennes par élévation
        </button>
      </li>
      <li>
        <button
          @click="activateTab(4)"
          class="inline-block p-4 rounded-t-lg border-b-2"
          :class="(isActiveTab(4)) ?
            'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' :
            'border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300'"
        >
          Températures min/max par ville
        </button>
      </li>
      <li>
        <button
          @click="activateTab(5)"
          class="inline-block p-4 rounded-t-lg border-b-2"
          :class="(isActiveTab(5)) ?
            'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' :
            'border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300'"
        >
          Températures et précipitations
        </button>
      </li>
    </ul>
    <mean-temperatures-by-periods v-if="tab === 1"/>
    <min-max-temperatures-by-periods v-else-if="tab === 2"/>
    <mean-temperatures-with-elevations-by-year v-else-if="tab === 3"/>
    <min-max-temperatures-by-city v-else-if="tab === 4"/>
    <mean-temperatures-and-precipitations-by-periods v-else-if="tab === 5" />
  </div>
</template>

<script>
import Header from "@/components/Header";
import MeanTemperaturesByPeriods from "@/components/MeanTemperaturesByPeriods";
import MinMaxTemperaturesByPeriods from "@/components/MinMaxTemperaturesByPeriods";
import MeanTemperaturesWithElevationsByYear from "@/components/MeanTemperaturesWithElevationsByYear";
import MinMaxTemperaturesByCity from "@/components/MinMaxTemperaturesByCity";
import MeanTemperaturesAndPrecipitationsByPeriods from "@/components/MeanTemperaturesAndPrecipitationsByPeriods";

export default {
  name: "Maps",
  components: {
    MeanTemperaturesAndPrecipitationsByPeriods,
    MinMaxTemperaturesByCity,
    MeanTemperaturesWithElevationsByYear, MinMaxTemperaturesByPeriods, MeanTemperaturesByPeriods, Header
  },
  data() {
    return {
      tab: 1,
    };
  },
  methods: {
    activateTab(tab) {
      this.tab = tab;
    },
    isActiveTab(tab) {
      return tab === this.tab
    }
  },
}
</script>

<style scoped>

</style>