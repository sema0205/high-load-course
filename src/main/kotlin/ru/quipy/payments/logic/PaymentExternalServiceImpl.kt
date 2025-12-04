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
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
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

    private val httpClientExecutor = Executors.newFixedThreadPool(
        parallelRequests.coerceAtLeast(16),
        NamedThreadFactory("http-client-$accountName")
    )

    private val dbExecutor = ThreadPoolExecutor(
        16,
        16,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(10000),
        NamedThreadFactory("payment-db-callback")
    )

    private val httpClientQueueSizeGauge: Gauge = Gauge
        .builder("payment_http_client_queue_size") { (httpClientExecutor as? ThreadPoolExecutor)?.queue?.size?.toDouble() ?: 0.0 }
        .description("Current size of the HTTP client request queue")
        .register(meterRegistry)

    private val dispatcher = Dispatcher(httpClientExecutor).apply {
        maxRequests = parallelRequests * 2
        maxRequestsPerHost = parallelRequests * 2
    }

    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .connectionPool(ConnectionPool(parallelRequests, 20, TimeUnit.SECONDS))
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .readTimeout(Duration.ofSeconds(30))
        .build()

    private val rateLimiter = LeakingBucketRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
        (rateLimitPerSec * 1.2).toInt()
    )

    private val semaphore = Semaphore(parallelRequests, true)

    private fun deadlineHandler(paymentId: UUID, transactionId: UUID, reason: String) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Deadline by reason: $reason")
        }
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId. Reason: $reason")
    }

    private fun semaphoreRequestAcquire(deadline: Long) =
        remainingMillis(deadline).takeIf { it > 0 }
            ?.let { semaphore.tryAcquire(it, TimeUnit.MILLISECONDS) }
            ?: false

    private fun remainingMillis(epocTime: Long) =
        System.currentTimeMillis().takeIf { it < epocTime }
            ?.let { epocTime - it }
            ?: 0

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val requestStartTime = System.currentTimeMillis()
        val transactionId = UUID.randomUUID()

        val acquired = semaphoreRequestAcquire(deadline)
        if (!acquired) {
            deadlineHandler(paymentId, transactionId, "Unable to acquire request semaphore")
            return
        }

        if (!rateLimiter.tick()) {
            semaphore.release()
            deadlineHandler(paymentId, transactionId, "Rate limit exceeded")
            return
        }

        requestCounter.increment()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        sendRequestAsync(paymentId, amount, requestStartTime, deadline, transactionId)
    }

    fun sendRequestAsync(
        paymentId: UUID,
        amount: Int,
        requestStartTime: Long,
        deadline: Long,
        transactionId: UUID
    ) {
        if (now() >= deadline) {
            logger.error("[$accountName] Deadline exceeded for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
            }
            finishPayment(false, "Deadline exceeded", requestStartTime, transactionId, paymentId)
            return
        }

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: java.io.IOException) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                            e
                        )
                        dbExecutor.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                            finishPayment(false, "Request timeout.", requestStartTime, transactionId, paymentId)
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        dbExecutor.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                            finishPayment(false, e.message, requestStartTime, transactionId, paymentId)
                        }
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

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    dbExecutor.submit {
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        finishPayment(body.result, body.message, requestStartTime, transactionId, paymentId)
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