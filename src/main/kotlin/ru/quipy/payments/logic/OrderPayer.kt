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
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        const val SUBMISSION_THREADS = 256
        const val ADMISSION_RATE_PER_SEC = 4000L
        const val ADMISSION_BUCKET_SIZE = 4000
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        SUBMISSION_THREADS,
        SUBMISSION_THREADS,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(5000),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

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

                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                }
            )
            createdAt
        } catch (e: RejectedExecutionException) {
            logger.warn("Payment submission rejected for paymentId=$paymentId, orderId=$orderId")
            null
        }
    }
}