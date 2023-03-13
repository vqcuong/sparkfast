package com.sparkfast.spark

import org.apache.spark.sql.Dataset

package object funcs {
  def unionAll[T](datasets: Dataset[T]*): Dataset[T] = {
    datasets.reduce(_ union _)
  }
}
