package com.sparkfast.spark.execution

import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.SqlDef
import org.apache.spark.sql.SparkSession

object QueryExecution extends LoggerMixin {
  def execute(spark: SparkSession, sqlDefs: List[SqlDef]): Unit = {
    for (sqlDef <- sqlDefs) {
      var msg = s"Executed spark sql: ${sqlDef.sql}"
      val df = spark.sql(sqlDef.sql)
      if (sqlDef.tempView != null) {
        msg += s" and assign to temp view: ${sqlDef.tempView}"
        df.createOrReplaceTempView(sqlDef.tempView)
      }
      log.info(msg)
    }
  }
}
