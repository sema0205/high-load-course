package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.*
import okhttp3.Protocol
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
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

    private val httpClientExecutor = ThreadPoolExecutor(
        10000,
        10000,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(),
        NamedThreadFactory("payment-http-client")
    )

    private val httpClientQueueSizeGauge: Gauge = Gauge
        .builder("payment_http_client_queue_size") { httpClientExecutor.queue.size.toDouble() }
        .description("Current size of the HTTP client request queue")
        .register(meterRegistry)

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher(httpClientExecutor).apply {
            maxRequests = 20000
            maxRequestsPerHost = 20000
        })
        .connectionPool(ConnectionPool(10000, 60, TimeUnit.SECONDS))
        .callTimeout(Duration.ofMillis(30000))
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
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

        sendRequestAsync(0, paymentId, amount, requestStartTime, deadline, transactionId)
    }

    fun sendRequestAsync(
        attempt: Int,
        paymentId: UUID,
        amount: Int,
        requestStartTime: Long,
        deadline: Long,
        transactionId: UUID
    ) {
        if (now() >= deadline) {
            logger.error("[$accountName] Deadline exceeded before attempt $attempt for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
            }
            finishPayment(false, "Deadline exceeded", requestStartTime, transactionId, paymentId)
            return
        }

        val timeout = Duration.ofMillis(30000)
        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$timeout")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: java.io.IOException) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt ${attempt + 1}",
                            e
                        )

                        if (attempt < MAX_ATTEMPTS - 1 && now() < deadline) {
                            logger.warn("[$accountName] SocketTimeout for payment $paymentId, retrying immediately")
                            retryCounter.increment()
                            sendRequestAsync(attempt + 1, paymentId, amount, requestStartTime, deadline, transactionId)
                        } else {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                            finishPayment(false, "Request timeout.", requestStartTime, transactionId, paymentId)
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                        finishPayment(false, e.message, requestStartTime, transactionId, paymentId)
                    }
                }
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${e.message}"
                        )
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt ${attempt + 1}, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                        finishPayment(true, body.message, requestStartTime, transactionId, paymentId)
                    } else if (body.message == TEMPORARY_ERROR && attempt < MAX_ATTEMPTS - 1 && now() < deadline) {
                        logger.warn("[$accountName] Temporary error for payment $paymentId, retrying immediately")
                        retryCounter.increment()
                        sendRequestAsync(attempt + 1, paymentId, amount, requestStartTime, deadline, transactionId)
                    } else if (body.message == TEMPORARY_ERROR && attempt < MAX_ATTEMPTS - 1 && now() >= deadline) {
                        logger.error("[$accountName] Not enough time for retry, deadline too close")
                        paymentESService.update(paymentId) {
                            it.logProcessing(
                                false,
                                now(),
                                transactionId,
                                reason = "Temporary error, no time for retry"
                            )
                        }
                        finishPayment(false, "Temporary error, no time for retry", requestStartTime, transactionId, paymentId)
                    } else {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = body.message)
                        }
                        finishPayment(false, body.message, requestStartTime, transactionId, paymentId)
                    }
                }
            }
        })
    }

    fun finishPayment(
        success: Boolean,
        reason: String?,
        requestStartTime: Long,
        transactionId: UUID,
        paymentId: UUID
    ) {
        val requestFinishTime = System.currentTimeMillis()
        requestLatency.record(requestFinishTime - requestStartTime, TimeUnit.MILLISECONDS)
        logger.info("[$accountName] Finished payment for txId: $transactionId, payment: $paymentId, success: $success, reason: $reason")
        semaphore.release()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()