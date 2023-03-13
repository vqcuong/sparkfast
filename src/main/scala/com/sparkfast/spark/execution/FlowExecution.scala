package com.sparkfast.spark.execution

import com.sparkfast.spark.app.config.{QueryStepDef, ReadStepDef, ShowStepDef, StepDef, WriteStepDef}
import org.apache.spark.sql.SparkSession

object FlowExecution {
  def execute(spark: SparkSession, flow: List[StepDef]): Unit = {
    for (step <- flow) {
      step.`type` match {
        case "query" => QueryExecution.execute(spark, step.asInstanceOf[QueryStepDef].sqls)
        case "read" => ReadExecution.execute(spark, step.asInstanceOf[ReadStepDef].sources)
        case "write" => WriteExecution.execute(spark, step.asInstanceOf[WriteStepDef].sinks)
        case "show" => ShowExecution.execute(spark, step.asInstanceOf[ShowStepDef].items)
      }
    }
  }
}
