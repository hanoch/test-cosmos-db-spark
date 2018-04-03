package com.esri.cosmosdb

import java.util.Date

case class Stopwatch(startTime: Date = new Date) {
  def timeMS: Long = (new Date).getTime - startTime.getTime
  def timeSec:Double = 0.001 * ((new Date).getTime - startTime.getTime)
  def logTime(caption: String = ""): Unit = println(caption + timeSec + " seconds")
}
