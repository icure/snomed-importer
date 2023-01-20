package com.icure.importer.utils

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

enum class ProcessStatus{ QUEUED, STARTED, COMPLETED, STOPPED }

data class Process(
    val id: String,
    val status: ProcessStatus,
    val started: Long,
    val eta: Long? = null,
    val stacktrace: String? = null
) {

    fun updateETA(total: Int, processed: Int) =
        if (processed > 0)
            copy(
                eta = (((System.currentTimeMillis() - started)/processed) * (maxOf(0, total-processed))) + System.currentTimeMillis()
            )
        else this

}

@Component
class ProcessCache {

    private val processCache: Cache<String, Process> = Caffeine.newBuilder()
        .expireAfterWrite(7, TimeUnit.DAYS)
        .build()

    fun getProcess(id: String) = processCache.getIfPresent(id)

    fun updateProcess(id: String, process: Process) = processCache.put(id, process)

}