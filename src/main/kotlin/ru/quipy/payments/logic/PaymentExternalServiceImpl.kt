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
import java.util.concurrent.RejectedExecutionException
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

        const val REQUEST_TIMEOUT_MS = 1100L
        const val DEADLINE_SAFETY_MS = 120L
        const val MIN_HTTP_TIMEOUT_MS = 150L

        const val HTTP_CLIENT_THREADS = 100
        const val DB_CALLBACK_THREADS = 100
    }

    private val requestCounter = Counter.builder("http_shop_payment_requests")
        .description("Total number of requests sent by shop to payment service")
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
        HTTP_CLIENT_THREADS,
        HTTP_CLIENT_THREADS,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(5000),
        NamedThreadFactory("payment-http-client")
    )

    private val dbExecutor = ThreadPoolExecutor(
        DB_CALLBACK_THREADS,
        DB_CALLBACK_THREADS,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(5000),
        NamedThreadFactory("payment-db-callback")
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

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val semaphore = Semaphore(parallelRequests, false)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")

        val requestStartTime = System.currentTimeMillis()
        val resultFuture = CompletableFuture<Boolean>()

        dispatchPaymentAsync(paymentId, amount, paymentStartedAt, requestStartTime, deadline, resultFuture)
        return resultFuture
    }

    private fun dispatchPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        requestStartTime: Long,
        deadline: Long,
        resultFuture: CompletableFuture<Boolean>
    ) {
        if (now() >= deadline - DEADLINE_SAFETY_MS) {
            logger.error("[$accountName] Deadline exceeded before dispatch for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), null, reason = "Deadline exceeded before dispatch")
            }
            finishPayment(false, "Deadline exceeded before dispatch", requestStartTime, null, paymentId, releaseSemaphore = false)
            resultFuture.complete(false)
            return
        }

        if (!semaphore.tryAcquire()) {
            logger.debug("[$accountName] No free slot for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), null, reason = "No free slot")
            }
            finishPayment(false, "No free slot", requestStartTime, null, paymentId, releaseSemaphore = false)
            resultFuture.complete(false)
            return
        }

        if (!rateLimiter.tick()) {
            semaphore.release()
            logger.debug("[$accountName] Rate limit exceeded for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), null, reason = "Rate limit exceeded")
            }
            finishPayment(false, "Rate limit exceeded", requestStartTime, null, paymentId, releaseSemaphore = false)
            resultFuture.complete(false)
            return
        }

        requestCounter.increment()
        val transactionId = UUID.randomUUID()

        try {
            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            submitDbTask {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }
            logger.debug("[$accountName] Submit: $paymentId , txId: $transactionId")
            sendRequestAsync(paymentId, amount, requestStartTime, deadline, transactionId, resultFuture)
        } catch (e: Exception) {
            logger.error("[$accountName] Failed to submit payment $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            finishPayment(false, e.message, requestStartTime, transactionId, paymentId)
            resultFuture.complete(false)
        }
    }

    fun sendRequestAsync(
        paymentId: UUID,
        amount: Int,
        requestStartTime: Long,
        deadline: Long,
        transactionId: UUID,
        completionFuture: CompletableFuture<Boolean>
    ) {
        val budgetMs = deadline - now() - DEADLINE_SAFETY_MS
        if (budgetMs <= 0) {
            logger.error("[$accountName] Deadline exceeded before request for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
            }
            finishPayment(false, "Deadline exceeded", requestStartTime, transactionId, paymentId)
            completionFuture.complete(false)
            return
        }

        val timeoutMs = minOf(REQUEST_TIMEOUT_MS, budgetMs)
        if (timeoutMs < MIN_HTTP_TIMEOUT_MS) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Insufficient time budget")
            }
            finishPayment(false, "Insufficient time budget", requestStartTime, transactionId, paymentId)
            completionFuture.complete(false)
            return
        }

        val timeout = Duration.ofMillis(timeoutMs)

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

        val httpResultFuture: CompletableFuture<HttpResult> = httpClient
            .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .handle { response, ex ->
                if (ex != null) {
                    HttpResult.Failed(ex.cause ?: ex)
                } else {
                    val body = parseExternalResponse(response, paymentId, transactionId)
                    HttpResult.Completed(body)
                }
            }

        httpResultFuture
            .thenAccept { result ->
                when (result) {
                    is HttpResult.Completed -> handleCompletedResponse(
                        result.response,
                        paymentId,
                        requestStartTime,
                        transactionId,
                        completionFuture
                    )

                    is HttpResult.Failed -> handleFailedResponse(
                        result.cause,
                        paymentId,
                        requestStartTime,
                        transactionId,
                        completionFuture
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
        paymentId: UUID,
        requestStartTime: Long,
        transactionId: UUID,
        resultFuture: CompletableFuture<Boolean>
    ) {
        when (cause) {
            is HttpTimeoutException, is SocketTimeoutException -> {
                logger.error(
                    "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                    cause
                )
                submitFinalization(paymentId, transactionId, false, "Request timeout.", requestStartTime)
                resultFuture.complete(false)
            }

            else -> {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", cause)
                submitFinalization(paymentId, transactionId, false, cause.message, requestStartTime)
                resultFuture.complete(false)
            }
        }
    }

    private fun handleCompletedResponse(
        body: ExternalSysResponse,
        paymentId: UUID,
        requestStartTime: Long,
        transactionId: UUID,
        resultFuture: CompletableFuture<Boolean>
    ) {
        logger.debug("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
        submitFinalization(paymentId, transactionId, body.result, body.message, requestStartTime)
        resultFuture.complete(body.result)
    }

    private fun submitFinalization(
        paymentId: UUID,
        transactionId: UUID,
        success: Boolean,
        reason: String?,
        requestStartTime: Long
    ) {
        submitDbTask {
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = reason)
            }
            finishPayment(success, reason, requestStartTime, transactionId, paymentId)
        }
    }

    private fun submitDbTask(task: () -> Unit) {
        try {
            dbExecutor.submit(task)
        } catch (_: RejectedExecutionException) {
            // Fail-safe: keep payment state consistent even under DB executor saturation.
            task()
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
        logger.debug("[$accountName] Finished payment for txId: $transactionId, payment: $paymentId, success: $success, reason: $reason")
        if (releaseSemaphore) {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()