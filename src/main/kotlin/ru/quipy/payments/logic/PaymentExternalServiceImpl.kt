package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.SocketTimeoutException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledThreadPoolExecutor
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

        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_ATTEMPTS = 1
        const val REQUEST_TIMEOUT_MS = 850L
        const val DISPATCH_RETRY_DELAY_MS = 5L
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
        16,
        16,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(50000),
        NamedThreadFactory("payment-http-client")
    )

    private val dbExecutor = ThreadPoolExecutor(
        500,
        500,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(10000),
        NamedThreadFactory("payment-db-callback")
    )

    private val dispatchExecutor = ScheduledThreadPoolExecutor(
        4,
        NamedThreadFactory("payment-dispatch")
    )

    private val httpClientQueueSizeGauge: Gauge = Gauge
        .builder("payment_http_client_queue_size") { httpClientExecutor.queue.size.toDouble() }
        .description("Current size of the HTTP client request queue")
        .register(meterRegistry)

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .executor(httpClientExecutor)
        .connectTimeout(Duration.ofSeconds(5))
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

        dispatchPaymentAsync(paymentId, amount, paymentStartedAt, requestStartTime, deadline)
    }

    private fun dispatchPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        requestStartTime: Long,
        deadline: Long
    ) {
        if (now() >= deadline) {
            logger.error("[$accountName] Deadline exceeded before dispatch for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), null, reason = "Deadline exceeded before dispatch")
            }
            finishPayment(false, "Deadline exceeded before dispatch", requestStartTime, null, paymentId, releaseSemaphore = false)
            return
        }

        if (!semaphore.tryAcquire()) {
            scheduleDispatch(paymentId, amount, paymentStartedAt, requestStartTime, deadline)
            return
        }

        if (!rateLimiter.tick()) {
            semaphore.release()
            scheduleDispatch(paymentId, amount, paymentStartedAt, requestStartTime, deadline)
            return
        }

        requestCounter.increment()
        val transactionId = UUID.randomUUID()

        try {
            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
            sendRequestAsync(0, paymentId, amount, requestStartTime, deadline, transactionId)
        } catch (e: Exception) {
            logger.error("[$accountName] Failed to submit payment $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            finishPayment(false, e.message, requestStartTime, transactionId, paymentId)
        }
    }

    private fun scheduleDispatch(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        requestStartTime: Long,
        deadline: Long
    ) {
        dispatchExecutor.schedule(
            {
                dispatchPaymentAsync(paymentId, amount, paymentStartedAt, requestStartTime, deadline)
            },
            DISPATCH_RETRY_DELAY_MS,
            TimeUnit.MILLISECONDS
        )
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

        val timeout = Duration.ofMillis(REQUEST_TIMEOUT_MS)

        val uri = URI("http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName" +
                "&token=$token" +
                "&accountName=$accountName" +
                "&transactionId=$transactionId" +
                "&paymentId=$paymentId" +
                "&amount=$amount" +
                "&timeout=$timeout")

        val httpRequest = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(timeout)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val resultFuture: CompletableFuture<HttpResult> = httpClient
            .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .handle { response, ex ->
                if (ex != null) {
                    HttpResult.Failed(ex.cause ?: ex)
                } else {
                    val body = parseExternalResponse(response, paymentId, transactionId)
                    HttpResult.Completed(body)
                }
            }

        resultFuture
            .thenAccept { result ->
                when (result) {
                    is HttpResult.Completed -> handleCompletedResponse(
                        result.response,
                        attempt,
                        paymentId,
                        amount,
                        requestStartTime,
                        deadline,
                        transactionId
                    )

                    is HttpResult.Failed -> handleFailedResponse(
                        result.cause,
                        attempt,
                        paymentId,
                        amount,
                        requestStartTime,
                        deadline,
                        transactionId
                    )
                }
            }
    }

    private fun parseExternalResponse(
        response: HttpResponse<String>,
        paymentId: UUID,
        transactionId: UUID
    ): ExternalSysResponse {
        return try {
            mapper.readValue(response.body(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error(
                "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${e.message}"
            )
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }
    }

    private fun handleFailedResponse(
        cause: Throwable,
        attempt: Int,
        paymentId: UUID,
        amount: Int,
        requestStartTime: Long,
        deadline: Long,
        transactionId: UUID
    ) {
        when (cause) {
            is HttpTimeoutException, is SocketTimeoutException -> {
                logger.error(
                    "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId, attempt ${attempt + 1}",
                    cause
                )

                if (attempt < MAX_ATTEMPTS - 1 && now() < deadline) {
                    logger.warn("[$accountName] SocketTimeout for payment $paymentId, retrying immediately")
                    retryCounter.increment()
                    sendRequestAsync(attempt + 1, paymentId, amount, requestStartTime, deadline, transactionId)
                } else {
                    submitFinalization(paymentId, transactionId, false, "Request timeout.", requestStartTime)
                }
            }

            else -> {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", cause)
                submitFinalization(paymentId, transactionId, false, cause.message, requestStartTime)
            }
        }
    }

    private fun handleCompletedResponse(
        body: ExternalSysResponse,
        attempt: Int,
        paymentId: UUID,
        amount: Int,
        requestStartTime: Long,
        deadline: Long,
        transactionId: UUID
    ) {
        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt ${attempt + 1}, succeeded: ${body.result}, message: ${body.message}")

        if (body.result) {
            submitFinalization(paymentId, transactionId, true, body.message, requestStartTime)
        } else if (body.message == TEMPORARY_ERROR && attempt < MAX_ATTEMPTS - 1 && now() < deadline) {
            logger.warn("[$accountName] Temporary error for payment $paymentId, retrying immediately")
            retryCounter.increment()
            sendRequestAsync(attempt + 1, paymentId, amount, requestStartTime, deadline, transactionId)
        } else if (body.message == TEMPORARY_ERROR && attempt < MAX_ATTEMPTS - 1 && now() >= deadline) {
            logger.error("[$accountName] Not enough time for retry, deadline too close")
            submitFinalization(paymentId, transactionId, false, "Temporary error, no time for retry", requestStartTime)
        } else {
            submitFinalization(paymentId, transactionId, false, body.message, requestStartTime)
        }
    }

    private fun submitFinalization(
        paymentId: UUID,
        transactionId: UUID,
        success: Boolean,
        reason: String?,
        requestStartTime: Long
    ) {
        dbExecutor.submit {
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = reason)
            }
            finishPayment(success, reason, requestStartTime, transactionId, paymentId)
        }
    }

    private sealed interface HttpResult {
        data class Completed(val response: ExternalSysResponse) : HttpResult
        data class Failed(val cause: Throwable) : HttpResult
    }

    fun finishPayment(
        success: Boolean,
        reason: String?,
        requestStartTime: Long,
        transactionId: UUID?,
        paymentId: UUID,
        releaseSemaphore: Boolean = true
    ) {
        val requestFinishTime = System.currentTimeMillis()
        requestLatency.record(requestFinishTime - requestStartTime, TimeUnit.MILLISECONDS)
        logger.info("[$accountName] Finished payment for txId: $transactionId, payment: $paymentId, success: $success, reason: $reason")
        if (releaseSemaphore) {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()