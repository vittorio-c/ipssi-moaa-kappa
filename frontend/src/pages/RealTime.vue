<template>
  <Header/>
  <div class="mx-auto w-5/6">
    <show-table :records="records"></show-table>
  </div>
</template>

<script>
import ShowTable from "@/components/ShowTable";
import Header from "@/components/Header";
import axios from "axios";

export default {
  name: "RealTime",
  data() {
    return {
      records: [],
    }
  },
  components: {ShowTable, Header},
  computed: {
    path() {
      return "http://localhost:8088/api/real-time/hourly"
    },
  },
  methods: {
    fetchApiData() {
      axios
        .get(this.path)
        .then((response) => {
          this.records = response.data._data;
        })
        .catch((error) => {
          // eslint-disable-next-line
          console.error(error);
        });
    },
  },
  mounted() {
    this.fetchApiData()
    window.setInterval(() => {
      console.log('Calling API again...')
      this.fetchApiData()
    }, 5000)
  }
}
</script>

<style scoped>

</style>