import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val kotlinCoroutinesVersion = "1.6.1"
val jacksonVersion = "2.12.5"

plugins {
    id("org.springframework.boot") version "2.6.7"
    id("io.spring.dependency-management") version "1.0.11.RELEASE"
    kotlin("jvm") version "1.6.21"
    kotlin("plugin.spring") version "1.6.21"
}

group = "com.icure.snomed"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
    mavenCentral()
    maven {
        url = uri("https://maven.taktik.be/content/groups/public")
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation(group = "com.fasterxml.jackson.core", name = "jackson-core", version = jacksonVersion)
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = jacksonVersion)
    implementation(group = "com.fasterxml.jackson.module", name = "jackson-module-kotlin", version = jacksonVersion)
    implementation(group = "com.fasterxml.jackson.datatype", name = "jackson-datatype-jsr310", version = jacksonVersion)

    implementation(group = "io.projectreactor.netty", name = "reactor-netty", version = "1.0.18")

    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-coroutines-core", version = kotlinCoroutinesVersion)
    implementation(
        group = "org.jetbrains.kotlinx",
        name = "kotlinx-coroutines-reactive",
        version = kotlinCoroutinesVersion
    )
    implementation(
        group = "org.jetbrains.kotlinx",
        name = "kotlinx-coroutines-reactor",
        version = kotlinCoroutinesVersion
    )

    implementation(group = "io.icure", name = "async-jackson-http-client", version = "0.1.18-6592ef18f1")
    implementation(
        group = "io.icure",
        name = "icure-reactive-kotlin-client",
        version = "0.1.420-bedcb22f02"
    )

    implementation(group = "org.jetbrains.kotlinx", name = "kotlinx-collections-immutable-jvm", version = "0.3.5")

    implementation("edu.stanford.nlp:stanford-corenlp:4.4.0")
    implementation("edu.stanford.nlp:stanford-corenlp:4.4.0:models")
    implementation("edu.stanford.nlp:stanford-corenlp:4.4.0:models-french")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(group = "io.kotest", name = "kotest-assertions-core", version = "4.6.4")
    testImplementation(group = "io.kotest", name = "kotest-runner-junit5", version = "4.6.4")

}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
