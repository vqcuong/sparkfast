package com.sparkfast.spark.source

import com.sparkfast.spark.app.config.SourceDef

object SourceInitializer {
  def makeSource(sourceDef: SourceDef): BaseSource = {
    val source: BaseSource = if (sourceDef.fromTable != null)
      new TableBasedSource(sourceDef) else new FileBasedSource(sourceDef)
    source.validate()
    source
  }
}
