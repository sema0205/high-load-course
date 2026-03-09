package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
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
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // ──────────────────────────────────────────────
    // HTTP client pool (round-robin, like friend's code)
    // ──────────────────────────────────────────────
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
    private val clientIndex = AtomicInteger(0)

    // ──────────────────────────────────────────────
    // Rate limiting & concurrency (non-blocking window + blocking rate limiter)
    // ──────────────────────────────────────────────
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    // ──────────────────────────────────────────────
    // DB executor — simple thread pool, no batching overhead
    // ──────────────────────────────────────────────
    private val dbExecutor = Executors.newFixedThreadPool(DB_POOL_SIZE)

    // ──────────────────────────────────────────────
    // Metrics
    // ──────────────────────────────────────────────
    private val paymentAttemptsTotal: Counter = Counter.builder("payment_attempts_total")
        .description("Payment attempts sent to provider")
        .register(meterRegistry)

    private val paymentSuccessTotal: Counter = Counter.builder("payment_success_total")
        .description("Successfully processed payments")
        .register(meterRegistry)

    private val paymentFailureTotal: Counter = Counter.builder("payment_failure_total")
        .description("Failed payments")
        .register(meterRegistry)

    private val paymentCompletedTotal: Counter = Counter.builder("payment_completed_total")
        .description("Payments completed total")
        .register(meterRegistry)

    private val paymentTimeoutCounter: Counter = Counter.builder("payment_timeout_total")
        .description("Total payment timeouts")
        .register(meterRegistry)

    private val requestLatency: Timer = Timer.builder("payment_request_latency")
        .description("Payment request latency")
        .publishPercentiles(0.5, 0.85, 0.99)
        .register(meterRegistry)

    // ──────────────────────────────────────────────
    // Main payment flow
    // ──────────────────────────────────────────────

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        val transactionId = UUID.randomUUID()
        val cf = CompletableFuture<Boolean>()
        val requestStartTime = now()

        paymentAttemptsTotal.increment()

        // ── Non-blocking concurrency check ──
        // If no slot available, reject immediately with NO DB write (avoids DB storm)
        if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
            logger.debug("[$accountName] No free slot for payment $paymentId, rejecting")
            cf.complete(false)
            return cf
        }

        logger.debug("[$accountName] Submitting payment $paymentId, txId: $transactionId")

        // ── Log submission asynchronously (fire-and-forget) ──
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

        try {
            // ── BLOCKING rate limiter — wait for slot instead of rejecting ──
            // This is the KEY difference: we wait until the rate limiter allows us through.
            // This means almost every payment that gets a concurrency slot WILL be sent.
            slidingWindowRateLimiter.tickBlocking()

            // ── Build request (no timeout param — let server decide) ──
            val urlString =
                "http://$paymentProviderHostPort/external/process" +
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

            // ── Round-robin client selection ──
            val idx = clientIndex.getAndIncrement()
            val client = clients[(idx and Int.MAX_VALUE) % clients.size]

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    try {
                        paymentFailureTotal.increment()
                        cf.complete(false)
                        recordLatency(requestStartTime)

                        if (e is SocketTimeoutException) {
                            paymentTimeoutCounter.increment()
                            logger.error(
                                "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e
                            )
                        } else {
                            logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e
                            )
                        }

                        dbExecutor.execute {
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(
                                        false, now(), transactionId,
                                        reason = if (e is SocketTimeoutException) "Request timeout." else e.message
                                    )
                                }
                            } catch (u: Exception) {
                                logger.error(
                                    "[$accountName] Error updating ES on failure for payment $paymentId", u
                                )
                            }
                        }
                    } finally {
                        releaseWindow()
                        paymentCompletedTotal.increment()
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        val bodyText = try {
                            response.body?.string()
                        } catch (_: Exception) {
                            null
                        }

                        val body = try {
                            mapper.readValue(bodyText, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            paymentFailureTotal.increment()
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

                        val result = body.result
                        if (result) {
                            paymentSuccessTotal.increment()
                        } else {
                            paymentFailureTotal.increment()
                        }

                        cf.complete(result)
                        recordLatency(requestStartTime)

                        dbExecutor.execute {
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                                }
                            } catch (u: Exception) {
                                logger.error(
                                    "[$accountName] Error updating ES on response for payment $paymentId", u
                                )
                            }
                        }
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e
                        )
                        paymentFailureTotal.increment()
                        cf.complete(false)
                        recordLatency(requestStartTime)

                        dbExecutor.execute {
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }
                            } catch (u: Exception) {
                                logger.error(
                                    "[$accountName] Error in exception handler for payment $paymentId", u
                                )
                            }
                        }
                    } finally {
                        releaseWindow()
                        paymentCompletedTotal.increment()
                    }
                }
            })
        } catch (e: Exception) {
            paymentFailureTotal.increment()
            recordLatency(requestStartTime)

            when (e) {
                is SocketTimeoutException -> {
                    paymentTimeoutCounter.increment()
                    logger.error(
                        "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e
                    )
                }
                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e
                    )
                }
            }

            cf.complete(false)

            dbExecutor.execute {
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                } catch (u: Exception) {
                    logger.error("[$accountName] Error updating ES in outer catch for $paymentId", u)
                }
            }

            releaseWindow()
            paymentCompletedTotal.increment()
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

    private fun recordLatency(requestStartTime: Long) {
        requestLatency.record(now() - requestStartTime, TimeUnit.MILLISECONDS)
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()