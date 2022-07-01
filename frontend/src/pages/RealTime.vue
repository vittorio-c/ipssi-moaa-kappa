<template>
  <Header/>
  <div class="mx-auto w-5/6">
    <h1 class="text-2xl">Temps réel à J-2, sur la journée du {{ twoDaysAgo }}</h1>
    <div class="w-full flex mt-4">
      <div class="w-1/2">
        <h2 class="text-l font-bold mb-4">Températures moyennes par heure</h2>
        <mean-temperatures-by-hour-real-time :records="meanRecords"></mean-temperatures-by-hour-real-time>
      </div>
      <div class="w-1/2">
        <h2 class="text-l font-bold mb-4">Températures min/max par heure</h2>
        <min-max-by-hour-real-time :records="minMaxRecords"></min-max-by-hour-real-time>
      </div>
    </div>
  </div>
</template>

<script>
import MeanTemperaturesByHourRealTime from "@/components/MeanTemperaturesByHourRealTime";
import Header from "@/components/Header";
import axios from "axios";
import MinMaxByHourRealTime from "@/components/MinMaxByHourRealTime";
import moment from "moment";

export default {
  name: "RealTime",
  data() {
    return {
      mean_records: [],
      min_max_records: [],
      mean_timer: null,
      min_max_timer: null
    }
  },
  components: {MinMaxByHourRealTime, MeanTemperaturesByHourRealTime, Header},
  computed: {
    twoDaysAgo() {
      moment.locale('fr');
      const today = moment();
      const twoDaysAgo = today.subtract(3, "days");

      return twoDaysAgo.format("dddd Do MMMM YYYY");
    },
    path() {
      return "http://localhost:8088/api/real-time/hourly"
    },
    meanRecords() {
      const today = moment();
      const twoDaysAgo = today.subtract(3, "days").format("YYYY-MM-DD");

      let dateRaw = {}
      for (let i = 0; i < 24; i++) {
        if (i < 10) {
          i = '0' + i
        }

        dateRaw[`${twoDaysAgo}T${i}:00:00`] = {
          "display_date": `${i}:00 - ${i}:59`,
          "mean_tmp": null,
        }
      }

      this.mean_records.forEach((r) => {
        dateRaw[r.reported_hour]["mean_tmp"] = r.mean_tmp
      })

      return dateRaw
    },
    minMaxRecords() {
      const today = moment();
      const twoDaysAgo = today.subtract(3, "days").format("YYYY-MM-DD");

      let dateRaw = {}
      for (let i = 0; i < 24; i++) {
        if (i < 10) {
          i = '0' + i
        }

        dateRaw[`${twoDaysAgo}T${i}:00:00`] = {
          "display_date": `${i}:00 - ${i}:59`,
          "min_tmp": null,
          "max_tmp": null,
        }
      }

      this.min_max_records.forEach((r) => {
        dateRaw[r.reported_hour]["min_tmp"] = r.min_tmp
        dateRaw[r.reported_hour]["max_tmp"] = r.max_tmp
      })

      return dateRaw
    }
  },
  methods: {
    fetchApiData() {
      axios
        .get(this.path)
        .then((response) => {
          this.mean_records = response.data._data;
          this.min_max_records = response.data._data;
        })
        .catch((error) => {
          // eslint-disable-next-line
          console.error(error);
        });
    }
  },
  mounted() {
    this.fetchApiData()

    this.mean_timer = setInterval(() => {
      this.fetchApiData()
    }, 5000)
  }
  ,

  beforeUnmount() {
    clearInterval(this.mean_timer)
  }
}
</script>

<style scoped>

</style>