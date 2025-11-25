package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        5000,
        5000,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(50_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit(
            ThreadPoolExecutorTask {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            orderId,
                            amount
                        )
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    val future = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)

                    future.whenCompleteAsync({ success, error ->
                        when {
                            error != null -> {
                                logger.error("Payment $paymentId failed with exception: ${error.message}")
                            }
                            success -> {
                                logger.info("Payment $paymentId succeeded")
                            }
                            else -> {
                                logger.warn("Payment $paymentId failed (no exception)")
                            }
                        }
                    }, paymentExecutor)

                } catch (e: Exception) {
                    logger.error("Failed to create payment for order $orderId", e)
                }
            })

            return createdAt
    }
}