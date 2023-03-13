package com.sparkfast.spark.execution

import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.{SqlDef, StorageLevelMapper, SupportedStorageLevel}
import org.apache.spark.sql.SparkSession

object QueryExecution extends LoggerMixin {

  def execute(spark: SparkSession, sqlDefs: List[SqlDef]): Unit = {
    for (sqlDef <- sqlDefs) {
      var msg = s"Executed spark sql: \n${sqlDef.sql}"
      val df = spark.sql(sqlDef.sql)
      if (sqlDef.tempView != null) {
        msg += s"\nand assign to temp view: ${sqlDef.tempView}"
        df.createOrReplaceTempView(sqlDef.tempView)
      }
      if (sqlDef.cache.isDefined) {
        if (sqlDef.cache.get) {
          df.cache()
          msg += " with enabling cache"
        }
      } else if (sqlDef.storageLevel != null && sqlDef.storageLevel != SupportedStorageLevel.NONE) {
        df.persist(StorageLevelMapper.get(sqlDef.storageLevel))
        msg += s" with persist on storage level: ${sqlDef.storageLevel.name()}"
      }
      log.info(msg)
    }
  }
}
