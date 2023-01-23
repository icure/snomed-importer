package com.icure.importer.controllers

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Scheduler
import com.icure.importer.loinc.LoincDownloadLogic
import com.icure.importer.scheduler.TaskScheduler
import com.icure.importer.snomed.SnomedDownloadLogic
import com.icure.importer.utils.Process
import com.icure.importer.utils.ProcessCache
import com.icure.importer.utils.ProcessStatus
import com.icure.importer.utils.basicAuth
import edu.stanford.nlp.ling.CoreAnnotations.DistSimAnnotation
import io.icure.kraken.client.apis.UserApi
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.mono
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.lang.IllegalArgumentException
import java.util.*
import java.util.concurrent.TimeUnit

data class CodificationParameters (
    val user: String,
    val pwd: String,
    val iCureUser: String,
    val iCurePwd: String,
    val iCureUrl: String
)

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
@RestController
@RequestMapping("/importer")
class DownloadController(
    val loincDownloadLogic: LoincDownloadLogic,
    val snomedDownloadLogic: SnomedDownloadLogic,
    val scheduler: TaskScheduler,
    val cache: ProcessCache
) {

    @PostMapping("/loinc")
    fun importLoincCodes(@RequestBody parameters: CodificationParameters) = mono {
        val user = UserApi(basePath = parameters.iCureUrl, authHeader = basicAuth(parameters.iCureUser, parameters.iCurePwd))
            .getCurrentUser()
        if(user.login != parameters.iCureUser) throw ResponseStatusException(HttpStatus.UNAUTHORIZED)
        val processId = UUID.randomUUID().toString()
        val task = loincDownloadLogic.generateTask(
            processId,
            parameters.user,
            parameters.pwd,
            parameters.iCureUrl,
            parameters.iCureUser,
            parameters.iCurePwd,
            100
        )
        scheduler.addToTaskQueue(task)
        processId
    }

    @OptIn(ExperimentalStdlibApi::class)
    @PostMapping("/snomed/{releaseType}/{region}")
    fun importSnomedCodes(
        @PathVariable releaseType: String,
        @PathVariable region: String,
        @RequestBody parameters: CodificationParameters
    ) = mono {
        val user = UserApi(basePath = parameters.iCureUrl, authHeader = basicAuth(parameters.iCureUser, parameters.iCurePwd))
            .getCurrentUser()
        if(user.login != parameters.iCureUser) throw ResponseStatusException(HttpStatus.UNAUTHORIZED)
        if(!listOf("delta", "snapshot").contains(releaseType)) throw IllegalArgumentException("releaseType should be either delta or snapshot")
        val regionCode = when(region) {
            "int" -> 167
            "be" -> 190440
            else -> throw IllegalArgumentException("region should be either int or be")
        }
        val processId = UUID.randomUUID().toString()
        cache.updateProcess(
            processId,
            Process(
                processId,
                ProcessStatus.QUEUED,
                System.currentTimeMillis(),
                message = "Waiting to start the process"
            )
        )
        val task = snomedDownloadLogic.generateTask(
            processId,
            regionCode,
            releaseType,
            parameters.user,
            parameters.pwd,
            parameters.iCureUrl,
            parameters.iCureUser,
            parameters.iCurePwd,
            1000
        )
        scheduler.addToTaskQueue(task)
        processId
    }

    @GetMapping("/job/{processId}")
    fun getProcessStatus(@PathVariable processId: String) = cache.getProcess(processId) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)

    @DeleteMapping("/job/{processId}")
    fun abortProcess(@PathVariable processId: String) =
        cache.getProcess(processId)?.let { process ->
            cache.updateProcess(
                processId,
                process.copy(
                    status = ProcessStatus.WAITING_FOR_TERMINATION,
                    eta = null,
                    message = "Process is scheduled for termination"
                )
            )
        } ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)

}