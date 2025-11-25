package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_ATTEMPTS = 3
        const val TEMPORARY_ERROR = "Temporary error"
    }

    private val requestCounter = Counter.builder("http_shop_payment_requests")
        .description("Total number of requests sent by shop to payment service")
        .register(meterRegistry)

    private val retryCounter = Counter.builder("payment_retry_count")
        .description("Number of payment retries")
        .register(meterRegistry)

    private val requestLatency = Timer.builder("payment_request_latency")
        .description("Payment request latency with quantiles")
        .publishPercentiles(0.5, 0.85, 0.99)
        .register(meterRegistry)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(30000))
        .readTimeout(Duration.ofMillis(30000))
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .dispatcher(Dispatcher().apply {
            maxRequests = 20000
            maxRequestsPerHost = 20000
        })
        .connectionPool(ConnectionPool(20000, 60, TimeUnit.SECONDS))
        .build()

    // Добавляем sliding window rate limiter на основе параметров аккаунта
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val semaphore = Semaphore(parallelRequests, true)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val requestStartTime = System.currentTimeMillis()

        // Semaphore — ограничиваем параллелизм
        semaphore.acquire()

        // Ждем разрешения от rate limiter (блокирующий вызов)
        rateLimiter.tickBlocking()

        requestCounter.increment()

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        executePaymentRequestAsync(
            paymentId,
            amount,
            paymentStartedAt,
            deadline,
            attempt = 1,
            transactionId,
            requestStartTime
        )
    }

    private fun executePaymentRequestAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int,
        transactionId: UUID,
        requestStartTime: Long
    ) {
        if (now() >= deadline) {
            logger.error("[$accountName] Deadline exceeded before attempt ${attempt} for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
            }

            completeRequest(requestStartTime)
            return
        }

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${Duration.ofMillis(30000)}")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    response.use { resp ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt ${attempt}, succeeded: ${body.result}, message: ${body.message}")

                        if (body.result) {
                            // Успех - завершаем
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }

                            completeRequest(requestStartTime)
                        } else if (body.message == TEMPORARY_ERROR) {
                            // Временная ошибка - retry если есть время
                            handleRetryableRequestError(
                                paymentId,
                                amount,
                                paymentStartedAt,
                                deadline,
                                attempt,
                                transactionId,
                                requestStartTime
                            )
                        } else {
                            // Постоянная ошибка или последняя попытка
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = body.message)
                            }

                            completeRequest(requestStartTime)
                        }
                    }
                } catch (e: Exception) {
                    logger.error("Error processing response", e)
                    completeRequest(requestStartTime)
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                if (e is SocketTimeoutException) {
                    logger.error(
                        "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt ${attempt}",
                        e
                    )

                    handleRetryableRequestError(
                        paymentId,
                        amount,
                        paymentStartedAt,
                        deadline,
                        attempt,
                        transactionId,
                        requestStartTime
                    )
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }

                    completeRequest(requestStartTime)
                }
            }
        })
    }

    private fun handleRetryableRequestError(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int,
        transactionId: UUID,
        requestStartTime: Long
    ) {
        val nextAttempt = attempt + 1
        if (nextAttempt > MAX_ATTEMPTS) {
            logger.error("[$accountName] Payment max attempts reached for txId: $transactionId, payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(
                    false,
                    now(),
                    transactionId,
                    reason = "Max attempts reached")
            }

            completeRequest(requestStartTime)
            return
        }

        val timeForRetry = now() + requestAverageProcessingTime.toMillis()

        if (timeForRetry < deadline) {
            logger.warn("[$accountName] Temporary error for payment $paymentId, retrying")
            retryCounter.increment()

            executePaymentRequestAsync(
                    paymentId = paymentId,
                    amount = amount,
                    paymentStartedAt = paymentStartedAt,
                    deadline = deadline,
                    attempt = nextAttempt,
                    transactionId = transactionId,
                    requestStartTime = requestStartTime
            )
        } else {
            logger.error("[$accountName] Not enough time for retry, deadline too close")
            paymentESService.update(paymentId) {
                it.logProcessing(
                    false,
                    now(),
                    transactionId,
                    reason = "Temporary error, no time for retry"
                )
            }

            completeRequest(requestStartTime)
        }
    }

    private fun completeRequest(requestStartTime: Long) {
        val requestFinishTime = System.currentTimeMillis()
        requestLatency.record(requestFinishTime - requestStartTime, TimeUnit.MILLISECONDS)

        semaphore.release()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()