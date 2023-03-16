package com.sparkfast.unittest

import com.sparkfast.spark.app.config.{AppConfLoader, ReadStepConf, SupportedSourceFormat, WriteStepConf}
import com.sparkfast.spark.source.{FileBasedSource, TableBasedSource}
import org.junit.jupiter.api.Test


class AppConfLoaderTest {
  @Test def testLoadingAppConfFromYaml(): Unit = {
    val appConf = AppConfLoader.loadFromResource("conf/application-example.yaml")
    assert(appConf.appName == "example")
  }

  /**
   * accept null value
   */
  @Test def testLoadingSourceConfWithValidNullFormat(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format:
          |        fromPath: /raw/kaggle/world_economy/corruption
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.format)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * accept value names of [[com.sparkfast.spark.app.config.SupportedSourceFormat]]
   * regardless of case sensitive
   */
  @Test def testLoadingSourceConfWithValidFormat(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: cSv
          |        fromPath: /raw/kaggle/world_economy/corruption
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.format)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * don't accept any value other than value names of [[com.sparkfast.spark.app.config.SupportedSourceFormat]]
   */
  @Test def testLoadingSourceConfWithInvalidFormat(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: rcfile
          |        fromPath: /raw/kaggle/world_economy/corruption
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.format)
    } catch {
      case _: Exception => success = false
    }
    assert(!success)
  }

  /**
   * accept null value
   */
  @Test def testLoadingSourceConfWithValidNullPath(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromPath:
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromPath)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * accept single non blank value of path
   */
  @Test def testLoadingSourceConfWithValidSinglePath(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromPath: /raw/kaggle/world_economy/corruption
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromPath)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * don't accept blank value
   */
  @Test def testLoadingSourceConfWithInvalidSinglePath(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromPath: '\t\n\r   '
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromPath)
    } catch {
      case _: Exception => success = false
    }
    assert(!success)
  }

  /**
   * accept a list without containing any null or blank value
   */
  @Test def testLoadingSourceConfWithValidMultiplePaths(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromPath:
          |          - corruption_0
          |          - world_economy/corruption_1
          |          - /raw/kaggle/world_economy/corruption_2
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromPath)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * don't accept a list containing any null or blank value
   */
  @Test def testLoadingSourceConfWithInvalidMultiplePaths(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromPath:
          |          -
          |          - "\t\n\r   "
          |          - corruption_0
          |          - world_economy/corruption_1
          |          - /raw/kaggle/world_economy/corruption_2
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromPath)
    } catch {
      case _: Exception => success = false
    }
    assert(!success)
  }

  /**
   * accept null value
   */
  @Test def testLoadingSourceConfWithValidNullTable(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable:
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromTable)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * accept non blank value
   */
  @Test def testLoadingSourceConfWithValidTable(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: world_economy.corruption
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromTable)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * don't accept non blank value
   */
  @Test def testLoadingSourceConfWithInvalidTable(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: "\t\n\r   "
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.fromTable)
    } catch {
      case _: Exception => success = false
    }
    assert(!success)
  }

  /**
   * accept null value
   */
  @Test def testLoadingSourceConfWithValidNullOptions(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: world_economy.corruption
          |        options:
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.options)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * only accept a map as value
   */
  @Test def testLoadingSourceConfWithValidMapOptions(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: world_economy.corruption
          |        options:
          |          header: true
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.options)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * doesn't accept any data type other than map
   */
  @Test def testLoadingSourceConfWithInvalidOptions(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: world_economy.corruption
          |        options:
          |          - header: true
          |          - sep: "|"
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.options)
    } catch {
      case _: Exception => success = false
    }
    assert(!success)
  }

  /**
   * accept null value
   */
  @Test def testLoadingSourceConfWithValidNullSchema(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: world_economy.corruption
          |        schema:
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.schema)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * accept non blank value
   */
  @Test def testLoadingSourceConfWithValidSchema(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: world_economy.corruption
          |        schema: |
          |          {"type":"struct","fields":[{"name":"country","type":"string","nullable":true,"metadata":{}},{"name":"annual_income","type":"integer","nullable":true,"metadata":{}},{"name":"corruption_index","type":"integer","nullable":true,"metadata":{}}]}
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.schema)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }

  /**
   * accept blank value as null value
   */
  @Test def testLoadingSourceConfWithInvalidSchema(): Unit = {
    val yamlContent =
      s"""appName: sparkfast-demo
          |enableHiveSupport: true
          |flow:
          |  - type: read
          |    sources:
          |      - format: csv
          |        fromTable: world_economy.corruption
          |        schema: "\t\n\r   "
          |        tempView: corruption""".stripMargin
    var success = true
    try {
      val appConf = AppConfLoader.loadFromString(yamlContent)
      val sourceConf = appConf.flow.head.asInstanceOf[ReadStepConf].sources.head
      println(sourceConf.schema)
    } catch {
      case _: Exception => success = false
    }
    assert(success)
  }
}
