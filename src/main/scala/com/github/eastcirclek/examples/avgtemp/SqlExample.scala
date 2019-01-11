package com.github.eastcirclek.examples.avgtemp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object SqlExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData: DataStream[(String, Long, Double)] = env.addSource(new RoomTempSource(25, 2, 5))

    val sensorTable: Table = sensorData
      .toTable(tableEnv, 'location, 'time, 'tempF)
      .window(Slide over 10.seconds every 1.second on 'time as 'w)
      .groupBy('location, 'w)
      .select('w.start as 'second, 'location,
        (('tempF.avg - 32) * 0.556) as 'avgTemfC)

    tableEnv.toAppendStream[Row](sensorTable).print()
    env.execute()
  }
}
