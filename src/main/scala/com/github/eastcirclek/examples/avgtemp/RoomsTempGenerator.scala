package com.github.eastcirclek.examples.avgtemp

import scala.util.Random

class RoomTempGenerator(id: Int, mean: Double, std: Double) {
  val roomId = s"room-$id"

  def generate(time: Long): (String, Long, Double) =
    (roomId, time, Random.nextGaussian*std+mean)
}

class RoomsTempGenerator(mean: Double, std: Double, numRooms: Int) {
  private val generators: Array[RoomTempGenerator] =
    Array.tabulate(numRooms){ roomId=>
      val avgRoomTemp = Random.nextGaussian()*std + mean
      new RoomTempGenerator(roomId, avgRoomTemp, Random.nextGaussian())
    }

  def generate(time: Long): Array[(String, Long, Double)] =
    generators map (_.generate(time))
}
