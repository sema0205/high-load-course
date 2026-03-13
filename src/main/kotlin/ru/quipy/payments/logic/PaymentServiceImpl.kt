package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Boolean> {
        val futures = paymentAccounts.map { account ->
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }

        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(false)
        }

        val result = CompletableFuture<Boolean>()
        val remaining = AtomicInteger(futures.size)

        futures.forEach { future ->
            future.whenComplete { success, _ ->
                if (success == true) {
                    result.complete(true)
                }
                if (remaining.decrementAndGet() == 0) {
                    result.complete(false)
                }
            }
        }

        return result
    }
}