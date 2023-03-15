package com.sparkfast.spark.source

import com.sparkfast.spark.app.config.SourceConf

object SourceInitializer {
  def makeSource(sourceConf: SourceConf): BaseSource = {
    val source: BaseSource = if (sourceConf.fromTable != null)
      new TableBasedSource(sourceConf) else new FileBasedSource(sourceConf)
    source.validate()
    source
  }
}
