package com.sparkfast.spark.execution

import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.ShowItemDef
import org.apache.spark.sql.SparkSession

object ShowExecution extends LoggerMixin {
  def execute(spark: SparkSession, showItemDefs: List[ShowItemDef]): Unit = {
    for (showItemDef <- showItemDefs) {
      var msg = ""
      val df = if (showItemDef.sql != null) {
        log.info(s"Show example data from sql: ${showItemDef.sql}")
        spark.sql(showItemDef.sql)
      } else if (showItemDef.table != null) {
        log.info(s"Show example data from table: ${showItemDef.table}")
        spark.read.table(showItemDef.table)
      } else if (showItemDef.tempView != null) {
        log.info(s"Show example data from temp view: ${showItemDef.tempView}")
        spark.sql(s"SELECT * FROM ${showItemDef.tempView}")
      } else {
        throw new RuntimeException("One of sql, table or viewName must be configured when step type is show")
      }
      if (df != null) {
        df.show(showItemDef.limit, truncate = false)
      }
    }
  }
}
