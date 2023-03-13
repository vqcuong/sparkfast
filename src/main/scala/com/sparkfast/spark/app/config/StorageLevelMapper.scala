package com.sparkfast.spark.app.config

import org.apache.spark.storage.StorageLevel


object StorageLevelMapper {
  private final lazy val storageLevelMap = Map(
    SupportedStorageLevel.NONE -> StorageLevel.NONE,
    SupportedStorageLevel.DISK_ONLY -> StorageLevel.DISK_ONLY,
    SupportedStorageLevel.DISK_ONLY_2 -> StorageLevel.DISK_ONLY_2,
    SupportedStorageLevel.DISK_ONLY_3 -> StorageLevel.DISK_ONLY_3,
    SupportedStorageLevel.MEMORY_ONLY -> StorageLevel.MEMORY_ONLY,
    SupportedStorageLevel.MEMORY_ONLY_2 -> StorageLevel.MEMORY_ONLY_2,
    SupportedStorageLevel.MEMORY_ONLY_SER -> StorageLevel.MEMORY_ONLY_SER,
    SupportedStorageLevel.MEMORY_ONLY_SER_2 -> StorageLevel.MEMORY_ONLY_SER_2,
    SupportedStorageLevel.MEMORY_AND_DISK -> StorageLevel.MEMORY_AND_DISK,
    SupportedStorageLevel.MEMORY_AND_DISK_2 -> StorageLevel.MEMORY_AND_DISK_2,
    SupportedStorageLevel.MEMORY_AND_DISK_SER -> StorageLevel.MEMORY_AND_DISK_SER,
    SupportedStorageLevel.MEMORY_AND_DISK_SER_2 -> StorageLevel.MEMORY_AND_DISK_SER_2,
    SupportedStorageLevel.OFF_HEAP -> StorageLevel.OFF_HEAP
  )

  def get(key: SupportedStorageLevel): StorageLevel = storageLevelMap.apply(key)
}
