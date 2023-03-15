package com.sparkfast.spark.execution

import com.sparkfast.spark.app.config.SourceConf
import com.sparkfast.spark.source.SourceInitializer
import org.apache.spark.sql.SparkSession

object ReadExecution {
  def execute(spark: SparkSession, sourceDefs: List[SourceConf]): Unit = {
    for (sourceDef <- sourceDefs) {
      val source = SourceInitializer.makeSource(sourceDef)
      source.load(spark)
    }
  }
}
