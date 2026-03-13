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
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        const val SUBMISSION_THREADS = 250
        const val ADMISSION_RATE_PER_SEC = 4000L
        const val ADMISSION_BUCKET_SIZE = 4000
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ScheduledThreadPoolExecutor(
        SUBMISSION_THREADS,
        NamedThreadFactory("payment-submission-executor")
    ).apply {
        maximumPoolSize = SUBMISSION_THREADS
        setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
        rejectedExecutionHandler = CallerBlockingRejectedExecutionHandler()
        setRemoveOnCancelPolicy(true)
    }

    private val dbExecutor = Executors.newFixedThreadPool(100)

    private val admissionLimiter = LeakingBucketRateLimiter(
        rate = ADMISSION_RATE_PER_SEC,
        window = Duration.ofSeconds(1),
        bucketSize = ADMISSION_BUCKET_SIZE
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        if (!admissionLimiter.tick()) {
            return null
        }

        dbExecutor.execute {
            try {
                paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.trace("Payment $paymentId for order $orderId created.")
            } catch (e: Exception) {
                logger.error("Payment $paymentId creation failed", e)
            }
        }

        paymentExecutor.submit {
            retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
        }

        return createdAt
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now

        if (timeLeft <= 0) {
            logger.warn("Payment $paymentId attempt #$attempt aborted: deadline exceeded")
            return
        }

        val future = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)

        future
            .orTimeout(timeLeft, TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->
                when {
                    error != null -> {
                        if (attempt <= 1) {
                            logger.warn(
                                "Payment $paymentId attempt #$attempt failed: ${error.message}, " +
                                        "timeLeft=${deadline - System.currentTimeMillis()}ms"
                            )
                        } else {
                            logger.debug("Payment $paymentId attempt #$attempt failed: ${error.message}")
                        }
                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }
                    success == true -> {
                        logger.info("Payment $paymentId attempt #$attempt succeeded")
                    }
                    success == false -> {
                        logger.info("Payment $paymentId attempt #$attempt returned failure")
                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }
                }
            }, paymentExecutor)
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now
        if (timeLeft <= 0) return

        val baseBackoff = (100L shl (attempt - 1)).coerceAtMost(2000L)
        val jitter = ThreadLocalRandom.current().nextLong(0, baseBackoff + 1)
        val delayMs = minOf(baseBackoff + jitter, timeLeft)

        paymentExecutor.schedule(
            { retryAsync(paymentId, amount, createdAt, deadline, attempt + 1) },
            delayMs,
            TimeUnit.MILLISECONDS
        )
    }
}