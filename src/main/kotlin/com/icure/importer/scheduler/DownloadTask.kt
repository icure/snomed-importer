package com.icure.importer.scheduler

interface DownloadTask {

    val processId: String
    suspend fun execute()

}