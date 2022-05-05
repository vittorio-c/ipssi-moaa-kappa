<template>
  <div class="mx-auto w-5/6">
    <h2 class="text-xl font-bold">Températures moyennes par stations pour {{ selectedYear }}</h2>
    <div class="my-4">Choisir une autre année :
      <select v-if="distinctYears" v-model="selectedYear">
        <option v-for="(year, key) in distinctYears" v-bind:key="key">
          {{ year }}
        </option>
      </select>

    </div>

    <div
      style="height: 75vh; width: 75vw;"
    >
      <l-map
        v-model="zoom"
        v-model:zoom="zoom"
        :center="[47.41322, -1.219482]"
        @move="log('move')"
      >
        <l-tile-layer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        ></l-tile-layer>
        <l-control-layers/>
        <div v-if="showCoordinates">
          <l-marker
            v-for="coordinate in geocordinates"
            v-bind:key="coordinate.stations_id"
            :lat-lng="[ coordinate.latitude, coordinate.longitude]"
            @moveend="log('moved marker')"
          >
            <l-tooltip>
              Température moyenne : {{ new Intl.NumberFormat().format(coordinate.mean_tmp) }} ° C
            </l-tooltip>
          </l-marker>
        </div>
      </l-map>
    </div>


  </div>
</template>
<script>
import {
  LMap,
  LTileLayer,
  LMarker,
  LControlLayers,
  LTooltip,
} from "@vue-leaflet/vue-leaflet";
import "leaflet/dist/leaflet.css";
import axios from "axios";

export default {
  name: "show-stations-by-year",
  components: {
    LMap,
    LTileLayer,
    LMarker,
    LControlLayers,
    LTooltip,
  },
  data() {
    return {
      zoom: 2,
      coordinates: [50, 50],
      iconWidth: 25,
      iconHeight: 40,
      geocordinates: [],
      year: 2018,
      distinctYears: [],
      selectedYear: 2018,
    };
  },
  computed: {
    path() {
      return "http://localhost:8088/api/geo/by-year/" + this.selectedYear
    },
    showCoordinates() {
      return Object.keys(this.coordinates).length > 0
    }
  },
  watch: {
    selectedYear() {
      this.fetchApiData()
    }
  },
  methods: {
    log(a) {
      console.log(a);
    },
    fetchApiData() {
      axios
        .get(this.path)
        .then((response) => {
          this.geocordinates = response.data._data;
          this.distinctYears = response.data._meta.distinct_years;
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
};
</script>

<style></style>