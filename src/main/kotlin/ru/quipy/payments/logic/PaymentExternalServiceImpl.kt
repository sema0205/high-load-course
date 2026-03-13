package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
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

    private val timeOut = Duration.ofSeconds(0)

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

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    private val dbExecutor = Executors.newFixedThreadPool(DB_POOL_SIZE)

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

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        val transactionId = UUID.randomUUID()
        val cf = CompletableFuture<Boolean>()

        paymentAttemptsTotal.increment()

        if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
            logger.debug("[$accountName] No free slot for payment $paymentId, rejecting")
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

        try {
            slidingWindowRateLimiter.tickBlocking()

            val urlString = if (timeOut != Duration.ofSeconds(0)) {
                "http://$paymentProviderHostPort/external/process" +
                        "?timeout=$timeOut" +
                        "&serviceName=$serviceName" +
                        "&token=$token" +
                        "&accountName=$accountName" +
                        "&transactionId=$transactionId" +
                        "&paymentId=$paymentId" +
                        "&amount=$amount"
            } else {
                "http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName" +
                        "&token=$token" +
                        "&accountName=$accountName" +
                        "&transactionId=$transactionId" +
                        "&paymentId=$paymentId" +
                        "&amount=$amount"
            }

            val request = Request.Builder()
                .url(urlString)
                .post(emptyBody)
                .build()

            val idx = clientIndex.getAndIncrement()
            val client = clients[(idx and Int.MAX_VALUE) % clients.size]

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    try {
                        paymentFailureTotal.increment()
                        cf.complete(false)

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

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()