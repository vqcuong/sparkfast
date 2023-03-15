package com.sparkfast.spark.execution

import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.{QueryStepConf, ReadStepConf, ShowStepConf, StepConf, WriteStepConf}
import org.apache.spark.sql.SparkSession

object FlowExecution extends LoggerMixin {
  def execute(spark: SparkSession, flow: List[StepConf]): Unit = {
    log.info(s"Number of steps: ${flow.length}")
    for (step <- flow) {
      step.`type` match {
        case "query" => QueryExecution.execute(spark, step.asInstanceOf[QueryStepConf].sqls)
        case "read" => ReadExecution.execute(spark, step.asInstanceOf[ReadStepConf].sources)
        case "write" => WriteExecution.execute(spark, step.asInstanceOf[WriteStepConf].sinks)
        case "show" => ShowExecution.execute(spark, step.asInstanceOf[ShowStepConf].items)
      }
    }
  }
}
