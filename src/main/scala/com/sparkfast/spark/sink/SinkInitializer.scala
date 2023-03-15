package com.sparkfast.spark.sink

import com.sparkfast.spark.app.config.SinkConf

object SinkInitializer {
  def makeSink(sinkConf: SinkConf): BaseSink = {
    val sink: BaseSink = if (sinkConf.toTable != null)
      new TableBasedSink(sinkConf) else new FileBasedSink(sinkConf)
    sink.validate()
    sink
  }
}
