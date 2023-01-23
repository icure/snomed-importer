package com.icure.importer.scheduler

import com.icure.importer.exceptions.ImportCanceledException
import com.icure.importer.utils.ProcessCache
import com.icure.importer.utils.ProcessStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import org.springframework.stereotype.Component
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.launch
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean

@Component
class TaskScheduler(
    val processCache: ProcessCache
) : InitializingBean, DisposableBean {

    private val taskExecutorScope = CoroutineScope(Dispatchers.Default)
    private val taskChannel = Channel<DownloadTask>(UNLIMITED)

    override fun afterPropertiesSet() {
        launchDownloadTasks()
    }

    override fun destroy() {
        taskExecutorScope.cancel()
    }

    suspend fun addToTaskQueue(task: DownloadTask) = taskChannel.send(task)

    private fun launchDownloadTasks() = taskExecutorScope.launch {
        for(task in taskChannel) {
            try {
                if(processCache.getProcess(task.processId)?.isCanceled() != false) throw ImportCanceledException()
                task.execute()
            } catch (e: Exception) {
                processCache.getProcess(task.processId)?.let {
                    processCache.updateProcess(
                        task.processId,
                        it.copy(
                            status = ProcessStatus.STOPPED,
                            eta = null,
                            stacktrace = e.stackTraceToString(),
                            message = e.message ?: "Process interrupted due to an error"
                        )
                    )
                }
            }
        }
    }


}