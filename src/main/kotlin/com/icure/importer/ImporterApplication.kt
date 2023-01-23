package com.icure.importer


import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.PropertySource
import java.io.File

@SpringBootApplication(scanBasePackages = [
    "com.icure.importer.controllers",
    "com.icure.importer.loinc",
    "com.icure.importer.snomed",
    "com.icure.importer.utils",
    "com.icure.importer.scheduler"
])
@PropertySource("classpath:application.properties")
@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
class ImporterApplication(

) {

    @Bean
    fun performStartupTasks(
        @Value("\${importer.base-folder}") basePath: String
    ) = ApplicationRunner {
        File(basePath).mkdirs()
    }

}

@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
fun main(args: Array<String>) {
    runApplication<ImporterApplication>(*args)
}
