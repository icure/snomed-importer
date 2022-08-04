package com.icure.snomed.importer

import java.lang.Integer.max
import java.util.concurrent.TimeUnit

class CommandlineProgressBar(
    private val message: String,
    private val maxCount: Int? = null,
    private val averageTimeWindowSize: Int = 10
) {
    private var count: Int = 0
    private var skipped: Int = 0
    private val deltas: Array<Long> = Array(averageTimeWindowSize) {0}
    private var i: Int = 0
    private var lastCheck: Long? = null
    private var hr: Long = 0
    private var min: Long = 0
    private var sec: Long = 0

    fun print() {
        val avgMillis = deltas.sum() / 10
        maxCount?.let {
            if (i == 0) {
                val total = avgMillis * (it - count)
                hr = TimeUnit.MILLISECONDS.toHours(total)
                min = TimeUnit.MILLISECONDS.toMinutes(total) - TimeUnit.HOURS.toMinutes(hr)
                sec =
                    TimeUnit.MILLISECONDS.toSeconds(total) - TimeUnit.MINUTES.toSeconds(min) - TimeUnit.HOURS.toSeconds(hr)
            }
            val progress = 20 * count / it
            print("$message ($avgMillis ms/it) [${"=".repeat(progress)}${".".repeat(max(20-progress, 0))}] $count/${it} - ETA: $hr:$min:$sec - $skipped skipped\r")
        } ?: print("$message ($avgMillis ms/it) - Processed: $count - $skipped skipped\r")
    }

    fun step(numSteps: Int = 1) {
        val steps = maxCount?.let{
            if(count + numSteps > it) numSteps
            else it - count
        } ?: numSteps
        val end = System.currentTimeMillis()
        lastCheck?.let {
            deltas[i] = (end - it)/steps
            i = (i+1)%averageTimeWindowSize
        }
        lastCheck = end
        count += steps
    }

    fun addSkip() {
        skipped++
    }
}