package com.github.eastcirclek.examples.avgtemp

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

class RoomTempSource(meanF: Int, stdF: Int, numRooms: Int) extends SourceFunction[(String, Long, Double)]{
  @volatile var running = true

  override def run(ctx: SourceFunction.SourceContext[(String, Long, Double)]): Unit = {
    val generator = new RoomsTempGenerator(meanF, stdF, numRooms)

    while(running) {
      val time = System.currentTimeMillis
      generator.generate(time).foreach(ctx.collectWithTimestamp(_, time))
      ctx.emitWatermark(new Watermark(time))

      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
