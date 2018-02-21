package com.esri.cosmosdb

object Plane {
  def createPlane: Plane = {
    val plane = new Plane(
      bearing = -70.72,
      dist = 5024.32,
      dest = "Frank Paris International Airport",
      lon = -31.88592,
      rtid = 1,
      ts = 1506957079, // 1506957079575
      secsToDep = -1,
      lat = 49.21297,
      speed = 240.25,
      id = "0",
      orig = "Mielec Airport")

    plane
  }
}


case class Plane (bearing: Double, dist: Double,  dest: String, lon: Double,
                  rtid: Int, ts: Long, secsToDep: Int,
                  lat: Double, speed: Double, id: String, orig: String) {
/*
  {
    "bearing": -70.72,
    "dest": "Frank Paris International Airport",
    "dist": 5024.32,
    "id": "0",
    "lat": 49.21297,
    "lon": -31.88592,
    "orig": "Mielec Airport",
    "rtid": 1,
    "secsToDep": -1,
    "speed": 240.25,
    "ts": 1506957079575
  }
*/

  def withId(id: String) = this.copy(id = id)
}
