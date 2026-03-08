package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.*
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        const val SUBMISSION_THREADS = 256
        const val ADMISSION_RATE_PER_SEC = 4000L
        const val ADMISSION_BUCKET_SIZE = 4000
        const val MAX_RETRY_ATTEMPTS = 1
        const val RETRY_BASE_DELAY_MS = 25L
        const val DEADLINE_SAFETY_MS = 120L
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ScheduledThreadPoolExecutor(
        SUBMISSION_THREADS,
        NamedThreadFactory("payment-submission-executor")
    )

    init {
        paymentExecutor.maximumPoolSize = SUBMISSION_THREADS
        paymentExecutor.rejectedExecutionHandler = ThreadPoolExecutor.AbortPolicy()
        paymentExecutor.setRemoveOnCancelPolicy(true)
    }

    private val admissionLimiter = LeakingBucketRateLimiter(
        rate = ADMISSION_RATE_PER_SEC,
        window = Duration.ofSeconds(1),
        bucketSize = ADMISSION_BUCKET_SIZE
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        if (!admissionLimiter.tick()) {
            return null
        }

        val createdAt = System.currentTimeMillis()
        return try {
            paymentExecutor.submit(
                ThreadPoolExecutorTask {
                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            orderId,
                            amount
                        )
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
                }
            )
            createdAt
        } catch (e: RejectedExecutionException) {
            logger.warn("Payment submission rejected for paymentId=$paymentId, orderId=$orderId")
            null
        }
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val timeLeft = deadline - System.currentTimeMillis()
        val budget = timeLeft - DEADLINE_SAFETY_MS
        if (budget <= 0) {
            return
        }

        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            .orTimeout(budget, TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->
                if (success == true) {
                    return@whenCompleteAsync
                }

                if (error != null) {
                    logger.debug("Payment $paymentId attempt #$attempt failed: ${error.message}")
                }

                if (attempt >= MAX_RETRY_ATTEMPTS || System.currentTimeMillis() >= deadline - DEADLINE_SAFETY_MS) {
                    return@whenCompleteAsync
                }

                scheduleRetry(paymentId, amount, createdAt, deadline, attempt + 1)
            }, paymentExecutor)
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        nextAttempt: Int
    ) {
        val timeLeft = deadline - System.currentTimeMillis()
        if (timeLeft <= 0) return

        val jitter = ThreadLocalRandom.current().nextLong(0, RETRY_BASE_DELAY_MS + 1)
        val delayMs = minOf(RETRY_BASE_DELAY_MS + jitter, timeLeft)

        paymentExecutor.schedule(
            { retryAsync(paymentId, amount, createdAt, deadline, nextAttempt) },
            delayMs,
            TimeUnit.MILLISECONDS
        )
    }
}