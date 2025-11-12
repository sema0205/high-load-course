package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import java.util.concurrent.RejectedExecutionException

data class ThreadPoolExecutorTask(var block: () -> Unit) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
    }

    override fun run() {
        try {
            block()
        } catch (rejected: RejectedExecutionException) {
            logger.error("PaymentTask execution rejected by abort policy: $this", rejected)
        } catch (e: Exception) {
            logger.error("PaymentTask failed unexpectedly: $this", e)
        }
    }
}