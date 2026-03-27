package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody: RequestBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        const val NUM_HTTP_CLIENTS = 15
        const val DB_POOL_SIZE = 200
        const val HEDGE_DELAY_MS = 200L
        const val MAX_HEDGES = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val requestAverageProcessingTime = properties.averageProcessingTime

    private val clients: List<OkHttpClient> = List(NUM_HTTP_CLIENTS) {
        val perClient = max(200, parallelRequests / 20)
        val exec = Executors.newFixedThreadPool(perClient)
        val dispatcher = Dispatcher(exec).apply {
            maxRequests = perClient
            maxRequestsPerHost = perClient
        }
        OkHttpClient.Builder()
            .dispatcher(dispatcher)
            .connectionPool(ConnectionPool(1, 10, TimeUnit.SECONDS))
            .readTimeout(Duration.ofSeconds(30))
            .retryOnConnectionFailure(true)
            .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
            .build()
    }

    private val circuitBreaker = CircuitBreaker.of(
        accountName,
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(10)
            .failureRateThreshold(50f)
            .slowCallRateThreshold(50f)
            .slowCallDurationThreshold(requestAverageProcessingTime.multipliedBy(2))
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .permittedNumberOfCallsInHalfOpenState(5)
            .minimumNumberOfCalls(10)
            .build()
    )

    private val clientIndex = AtomicInteger(0)

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    private val dbExecutor = Executors.newFixedThreadPool(DB_POOL_SIZE)

    private val hedgeScheduler = Executors.newScheduledThreadPool(4)

    private val requestCounter = Counter.builder("http_shop_payment_requests")
        .description("Total number of requests sent by shop to payment service")
        .register(meterRegistry)

    private val requestLatency = Timer.builder("payment_request_latency")
        .description("Payment request latency with quantiles")
        .publishPercentiles(0.5, 0.85, 0.99)
        .register(meterRegistry)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        val transactionId = UUID.randomUUID()
        val cf = CompletableFuture<Boolean>()
        val requestStartTime = now()
        requestCounter.increment()

        if (!circuitBreaker.tryAcquirePermission()) {
            logger.warn("[$accountName] Circuit breaker OPEN, rejecting $paymentId")
            dbExecutor.execute {
                try {
                    paymentESService.update(paymentId) {
                        it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                    }
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Circuit breaker is OPEN")
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Error logging CB rejection for $paymentId", e)
                }
            }
            cf.complete(false)
            return cf
        }

        if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
            logger.debug("[$accountName] No free slot for payment $paymentId, rejecting")
            circuitBreaker.releasePermission()
            cf.complete(false)
            return cf
        }

        logger.debug("[$accountName] Submitting payment $paymentId, txId: $transactionId")

        dbExecutor.execute {
            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(
                        success = true,
                        transactionId,
                        now(),
                        Duration.ofMillis(now() - paymentStartedAt)
                    )
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Error logging submission for $paymentId", e)
            }
        }

        val pendingHedges = AtomicInteger(MAX_HEDGES)
        val resultLogged = AtomicBoolean(false)
        val cbReported = AtomicBoolean(false)

        fun logResult(success: Boolean, reason: String?) {
            dbExecutor.execute {
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(success, now(), transactionId, reason = reason)
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Error updating ES for $paymentId", e)
                }
            }
        }

        fun reportCb(success: Boolean, durationMs: Long, error: Exception? = null) {
            if (cbReported.compareAndSet(false, true)) {
                if (success) {
                    circuitBreaker.onSuccess(durationMs, TimeUnit.MILLISECONDS)
                } else {
                    circuitBreaker.onError(durationMs, TimeUnit.MILLISECONDS, error ?: RuntimeException("failed"))
                }
            }
        }

        fun finalize(callSuccess: Boolean, durationMs: Long, error: Exception? = null) {
            reportCb(callSuccess, durationMs, error)
            if (pendingHedges.decrementAndGet() == 0) {
                requestLatency.record(now() - requestStartTime, TimeUnit.MILLISECONDS)
                releaseWindow()
                if (cf.complete(false) && resultLogged.compareAndSet(false, true)) {
                    logResult(false, "All hedged requests failed or were skipped")
                }
            }
        }

        fun sendHedge() {
            if (cf.isDone) {
                pendingHedges.decrementAndGet()
                return
            }

            if (!slidingWindowRateLimiter.tick()) {
                finalize(false, 0, RuntimeException("Rate limit exceeded"))
                return
            }

            val hedgeStart = now()

            val urlString = "http://$paymentProviderHostPort/external/process" +
                    "?serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount"

            val request = Request.Builder()
                .url(urlString)
                .post(emptyBody)
                .build()

            val idx = clientIndex.getAndIncrement()
            val usedClient = clients[(idx and Int.MAX_VALUE) % clients.size]

            try {
                usedClient.newCall(request).enqueue(object : Callback {
                    override fun onFailure(call: Call, e: IOException) {
                        try {
                            logger.error(
                                "[$accountName] Payment request failed for txId: $transactionId, payment: $paymentId", e
                            )
                        } finally {
                            finalize(false, now() - hedgeStart, e)
                        }
                    }

                    override fun onResponse(call: Call, response: Response) {
                        var callSuccess = false
                        try {
                            val bodyText = try {
                                response.body?.string()
                            } catch (_: Exception) {
                                null
                            }

                            val body = try {
                                mapper.readValue(bodyText, ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error(
                                    "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                                            "payment: $paymentId, result code: ${response.code}, reason: $bodyText", e
                                )
                                ExternalSysResponse(
                                    transactionId.toString(),
                                    paymentId.toString(),
                                    false,
                                    e.message ?: bodyText
                                )
                            }

                            logger.debug(
                                "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                        "succeeded: ${body.result}, message: ${body.message}"
                            )

                            callSuccess = body.result
                            if (body.result && cf.complete(true) && resultLogged.compareAndSet(false, true)) {
                                logResult(true, body.message)
                            }
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e
                            )
                        } finally {
                            finalize(callSuccess, now() - hedgeStart)
                        }
                    }
                })
            } catch (e: Exception) {
                logger.error("[$accountName] Error enqueueing request for $paymentId", e)
                finalize(false, now() - hedgeStart, e)
            }
        }

        sendHedge()

        for (i in 1 until MAX_HEDGES) {
            val delayMs = i * HEDGE_DELAY_MS
            if (deadline - now() > delayMs) {
                hedgeScheduler.schedule({ sendHedge() }, delayMs, TimeUnit.MILLISECONDS)
            } else {
                pendingHedges.decrementAndGet()
            }
        }

        return cf
    }

    private fun releaseWindow() {
        try {
            ongoingWindow.releaseWindow()
        } catch (u: Exception) {
            logger.error("[$accountName] Error releasing ongoingWindow", u)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
