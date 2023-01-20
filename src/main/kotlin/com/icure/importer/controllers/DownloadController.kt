package com.icure.importer.controllers

import com.icure.importer.loinc.LoincDownloadLogic
import com.icure.importer.utils.Process
import com.icure.importer.utils.ProcessCache
import com.icure.importer.utils.ProcessStatus
import com.icure.importer.utils.basicAuth
import io.icure.kraken.client.apis.UserApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.mono
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.util.*
import javax.websocket.server.PathParam

data class LoincParameters (
    val loincUser: String,
    val loincPwd: String,
    val iCureUser: String,
    val iCurePwd: String,
    val iCureUrl: String
)

@RestController
@RequestMapping("/importer")
class DownloadController(
    val loincDownloadLogic: LoincDownloadLogic,
    val cache: ProcessCache
) {
    @OptIn(ExperimentalStdlibApi::class)
    @PostMapping("/loinc")
    fun importLoincCodes(@RequestBody parameters: LoincParameters) = mono {
        val user = UserApi(basePath = parameters.iCureUrl, authHeader = basicAuth(parameters.iCureUser, parameters.iCurePwd))
            .getCurrentUser()
        if(user.login != parameters.iCureUser) throw ResponseStatusException(HttpStatus.UNAUTHORIZED)
        val processId = UUID.randomUUID().toString()
        CoroutineScope(IO).launch {
            cache.updateProcess(
                processId,
                Process(
                    processId,
                    ProcessStatus.QUEUED,
                    System.currentTimeMillis()
                )
            )
            try {
                loincDownloadLogic.updateLoincCodes(
                    processId,
                    parameters.loincUser,
                    parameters.loincPwd,
                    parameters.iCureUrl,
                    parameters.iCureUser,
                    parameters.iCurePwd
                )
                cache.getProcess(processId)?.let {
                    cache.updateProcess(
                        processId,
                        it.copy(
                            status = ProcessStatus.COMPLETED,
                            eta = System.currentTimeMillis()
                        )
                    )
                }
            } catch(e: Exception) {
                cache.getProcess(processId)?.let {
                    cache.updateProcess(
                        processId,
                        it.copy(
                            status = ProcessStatus.STOPPED,
                            eta = null,
                            stacktrace = e.stackTraceToString()
                        )
                    )
                }
            }
        }
        processId
    }

    @GetMapping("/check/{processId}")
    fun getProcessStatus(@PathVariable processId: String) = cache.getProcess(processId) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)

}