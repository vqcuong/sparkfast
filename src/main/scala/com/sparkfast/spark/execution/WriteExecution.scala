package com.sparkfast.spark.execution

import com.sparkfast.spark.app.config.SinkDef
import com.sparkfast.spark.sink.SinkInitializer
import org.apache.spark.sql.SparkSession

object WriteExecution {
  def execute(spark: SparkSession, sinkDefs: List[SinkDef]): Unit = {
    for (sinkDef <- sinkDefs) {
      val sink = SinkInitializer.makeSink(sinkDef)
      sink.save(spark)
    }
  }
}
