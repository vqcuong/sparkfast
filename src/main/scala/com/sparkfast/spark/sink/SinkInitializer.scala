package com.sparkfast.spark.sink

import com.sparkfast.spark.app.config.SinkDef

object SinkInitializer {
  def makeSink(sinkDef: SinkDef): BaseSink = {
    val sink: BaseSink = if (sinkDef.toTable != null)
      new TableBasedSink(sinkDef) else new FileBasedSink(sinkDef)
    sink.validate()
    sink
  }
}
