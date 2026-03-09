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
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

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
        const val HTTP_CLIENT_THREADS = 120
        // Reduced DB threads — we batch now, so fewer threads do more work
        const val DB_WORKER_THREADS = 40
        // Batch settings
        const val DB_BATCH_SIZE = 50
        const val DB_BATCH_FLUSH_INTERVAL_MS = 15L
        const val DB_RETRY_ATTEMPTS = 3
        const val DB_RETRY_DELAY_MS = 50L
    }

    // ──────────────────────────────────────────────
    // Metrics
    // ──────────────────────────────────────────────
    private val requestCounter = Counter.builder("http_shop_payment_requests")
        .description("Total number of requests sent by shop to payment service")
        .register(meterRegistry)

    private val requestLatency = Timer.builder("payment_request_latency")
        .description("Payment request latency with quantiles")
        .publishPercentiles(0.5, 0.85, 0.99)
        .register(meterRegistry)

    private val dbQueueSizeGauge = Gauge
        .builder("payment_db_queue_size") { dbTaskQueue.size.toDouble() }
        .description("Current size of the DB task queue")
        .register(meterRegistry)

    private val dbBatchCounter = Counter.builder("payment_db_batches")
        .description("Number of DB batches processed")
        .register(meterRegistry)

    private val dbFailureCounter = Counter.builder("payment_db_failures")
        .description("Number of DB task failures after retries")
        .register(meterRegistry)

    // ──────────────────────────────────────────────
    // Config
    // ──────────────────────────────────────────────
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // ──────────────────────────────────────────────
    // HTTP infrastructure
    // ──────────────────────────────────────────────
    private val httpClientExecutor = ThreadPoolExecutor(
        HTTP_CLIENT_THREADS,
        HTTP_CLIENT_THREADS,
        0, TimeUnit.SECONDS,
        LinkedBlockingQueue(8000),
        NamedThreadFactory("payment-http-client")
    )

    private val httpClientQueueSizeGauge: Gauge = Gauge
        .builder("payment_http_client_queue_size") { httpClientExecutor.queue.size.toDouble() }
        .description("Current size of the HTTP client request queue")
        .register(meterRegistry)

    private val httpClient: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .executor(httpClientExecutor)
        .connectTimeout(Duration.ofSeconds(3))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val semaphore = Semaphore(parallelRequests, false)

    // ──────────────────────────────────────────────
    // DB task batching infrastructure
    // ──────────────────────────────────────────────

    /**
     * Represents a single DB operation to be executed.
     * We separate submission and processing logs so we can
     * potentially combine or prioritize them.
     */
    data class DbTask(
        val paymentId: UUID,
        val operation: () -> Unit,
        val priority: Int = 0, // 0 = submission (fire-and-forget), 1 = finalization (important)
        val retryCount: AtomicInteger = AtomicInteger(0)
    )

    private val dbTaskQueue = LinkedBlockingQueue<DbTask>(20_000)

    // Worker threads that drain the queue in batches
    private val dbWorkerExecutor = ThreadPoolExecutor(
        DB_WORKER_THREADS,
        DB_WORKER_THREADS,
        0, TimeUnit.SECONDS,
        LinkedBlockingQueue(100),
        NamedThreadFactory("payment-db-worker")
    )

    // Single scheduler for periodic flush
    private val dbFlushScheduler = Executors.newSingleThreadScheduledExecutor(
        NamedThreadFactory("payment-db-flush")
    )

    init {
        // Start batch flush loop
        dbFlushScheduler.scheduleAtFixedRate(
            { drainAndProcessBatch() },
            DB_BATCH_FLUSH_INTERVAL_MS,
            DB_BATCH_FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        )
    }

    /**
     * Drains up to DB_BATCH_SIZE tasks from the queue and
     * submits them to the worker pool for parallel execution.
     * This is called periodically and also when the queue is large.
     */
    private fun drainAndProcessBatch() {
        try {
            val batch = mutableListOf<DbTask>()
            dbTaskQueue.drainTo(batch, DB_BATCH_SIZE)
            if (batch.isEmpty()) return

            dbBatchCounter.increment()

            // Submit each task in the batch to the worker pool
            // This allows parallel DB writes within a batch
            for (task in batch) {
                try {
                    dbWorkerExecutor.submit {
                        executeDbTaskWithRetry(task)
                    }
                } catch (_: RejectedExecutionException) {
                    // Worker pool full — execute inline
                    executeDbTaskWithRetry(task)
                }
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Error in DB batch flush", e)
        }
    }

    /**
     * Executes a DB task with retry logic.
     * Retries with exponential backoff on failure.
     */
    private fun executeDbTaskWithRetry(task: DbTask) {
        val maxRetries = DB_RETRY_ATTEMPTS
        while (true) {
            try {
                task.operation()
                return
            } catch (e: Exception) {
                val attempt = task.retryCount.incrementAndGet()
                if (attempt >= maxRetries) {
                    dbFailureCounter.increment()
                    logger.error(
                        "[$accountName] DB task failed after $maxRetries retries for payment ${task.paymentId}",
                        e
                    )
                    return
                }
                // Exponential backoff: 50ms, 100ms, 200ms...
                val delay = DB_RETRY_DELAY_MS * (1L shl (attempt - 1))
                logger.warn(
                    "[$accountName] DB task retry $attempt/$maxRetries for payment ${task.paymentId}, " +
                            "waiting ${delay}ms: ${e.message}"
                )
                try {
                    Thread.sleep(delay)
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return
                }
            }
        }
    }

    /**
     * Enqueues a DB task. Non-blocking — returns immediately.
     * If queue is full, drops low-priority tasks (submissions)
     * and executes high-priority tasks (finalizations) inline.
     */
    private fun enqueueDbTask(paymentId: UUID, priority: Int = 0, operation: () -> Unit) {
        val task = DbTask(paymentId, operation, priority)
        if (!dbTaskQueue.offer(task)) {
            if (priority >= 1) {
                // Finalization is critical — execute inline rather than drop
                logger.warn("[$accountName] DB queue full, executing finalization inline for $paymentId")
                try {
                    operation()
                } catch (e: Exception) {
                    logger.error("[$accountName] Inline DB task failed for $paymentId", e)
                }
            } else {
                logger.warn("[$accountName] DB queue full, dropping submission log for $paymentId")
            }
        }

        // If queue is getting large, trigger an extra drain
        if (dbTaskQueue.size > DB_BATCH_SIZE * 2) {
            try {
                dbWorkerExecutor.submit { drainAndProcessBatch() }
            } catch (_: RejectedExecutionException) {
                // Already busy — that's fine, scheduled flush will handle it
            }
        }
    }

    // ──────────────────────────────────────────────
    // Payment flow
    // ──────────────────────────────────────────────

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        logger.debug("[$accountName] Submitting payment request for payment $paymentId")
        val requestStartTime = now()
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
        // ── Check deadline ──
        if (now() >= deadline - DEADLINE_SAFETY_MS) {
            logger.error("[$accountName] Deadline exceeded before dispatch for payment $paymentId")
            // Fire-and-forget DB update — don't block the caller
            enqueueDbTask(paymentId, priority = 1) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), null, reason = "Deadline exceeded before dispatch")
                }
            }
            recordFinish(false, "Deadline exceeded before dispatch", requestStartTime, null, paymentId, releaseSemaphore = false)
            resultFuture.complete(false)
            return
        }

        // ── Acquire concurrency slot ──
        if (!semaphore.tryAcquire()) {
            logger.debug("[$accountName] No free slot for payment $paymentId")
            enqueueDbTask(paymentId, priority = 1) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), null, reason = "No free slot")
                }
            }
            recordFinish(false, "No free slot", requestStartTime, null, paymentId, releaseSemaphore = false)
            resultFuture.complete(false)
            return
        }

        // ── Rate limit ──
        if (!rateLimiter.tick()) {
            semaphore.release()
            logger.debug("[$accountName] Rate limit exceeded for payment $paymentId")
            enqueueDbTask(paymentId, priority = 1) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), null, reason = "Rate limit exceeded")
                }
            }
            recordFinish(false, "Rate limit exceeded", requestStartTime, null, paymentId, releaseSemaphore = false)
            resultFuture.complete(false)
            return
        }

        requestCounter.increment()
        val transactionId = UUID.randomUUID()

        try {
            // ── Log submission asynchronously (fire-and-forget, low priority) ──
            // This is the KEY optimization: we don't block here waiting for DB.
            // The submission log is enqueued and will be written in the next batch.
            enqueueDbTask(paymentId, priority = 0) {
                paymentESService.update(paymentId) {
                    it.logSubmission(
                        success = true,
                        transactionId,
                        now(),
                        Duration.ofMillis(now() - paymentStartedAt)
                    )
                }
            }

            logger.debug("[$accountName] Submit: $paymentId, txId: $transactionId")

            // ── Send HTTP request immediately (don't wait for DB) ──
            sendRequestAsync(paymentId, amount, requestStartTime, deadline, transactionId, resultFuture)
        } catch (e: Exception) {
            logger.error("[$accountName] Failed to submit payment $paymentId", e)
            enqueueDbTask(paymentId, priority = 1) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
            recordFinish(false, e.message, requestStartTime, transactionId, paymentId)
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
            enqueueDbTask(paymentId, priority = 1) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                }
            }
            recordFinish(false, "Deadline exceeded", requestStartTime, transactionId, paymentId)
            completionFuture.complete(false)
            return
        }

        val timeoutMs = minOf(REQUEST_TIMEOUT_MS, budgetMs)
        if (timeoutMs < MIN_HTTP_TIMEOUT_MS) {
            enqueueDbTask(paymentId, priority = 1) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Insufficient time budget")
                }
            }
            recordFinish(false, "Insufficient time budget", requestStartTime, transactionId, paymentId)
            completionFuture.complete(false)
            return
        }

        val timeout = Duration.ofMillis(timeoutMs)
        val uri = URI(
            "http://$paymentProviderHostPort/external/process" +
                    "?serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount" +
                    "&timeout=$timeout"
        )

        val httpRequest = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(timeout)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        httpClient
            .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .handle { response, ex ->
                if (ex != null) {
                    HttpResult.Failed(ex.cause ?: ex)
                } else {
                    val body = parseExternalResponse(response, paymentId, transactionId)
                    HttpResult.Completed(body)
                }
            }
            .thenAccept { result ->
                when (result) {
                    is HttpResult.Completed -> handleCompletedResponse(
                        result.response, paymentId, requestStartTime, transactionId, completionFuture
                    )

                    is HttpResult.Failed -> handleFailedResponse(
                        result.cause, paymentId, requestStartTime, transactionId, completionFuture
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
                "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                        "payment: $paymentId, result code: ${response.statusCode()}, reason: ${e.message}"
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
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", cause)
                submitFinalization(paymentId, transactionId, false, "Request timeout.", requestStartTime)
            }

            else -> {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", cause)
                submitFinalization(paymentId, transactionId, false, cause.message, requestStartTime)
            }
        }
        resultFuture.complete(false)
    }

    private fun handleCompletedResponse(
        body: ExternalSysResponse,
        paymentId: UUID,
        requestStartTime: Long,
        transactionId: UUID,
        resultFuture: CompletableFuture<Boolean>
    ) {
        logger.debug(
            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                    "succeeded: ${body.result}, message: ${body.message}"
        )
        submitFinalization(paymentId, transactionId, body.result, body.message, requestStartTime)
        resultFuture.complete(body.result)
    }

    /**
     * Enqueues the finalization DB write (high priority) and records metrics.
     * The semaphore is released immediately so the slot is freed for the next request
     * BEFORE the DB write completes — this is the second key optimization.
     */
    private fun submitFinalization(
        paymentId: UUID,
        transactionId: UUID,
        success: Boolean,
        reason: String?,
        requestStartTime: Long
    ) {
        // Release semaphore FIRST — free the slot immediately
        semaphore.release()

        // Record latency metric immediately
        val elapsed = now() - requestStartTime
        requestLatency.record(elapsed, TimeUnit.MILLISECONDS)

        // Enqueue DB write (high priority, will be retried if it fails)
        enqueueDbTask(paymentId, priority = 1) {
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = reason)
            }
        }

        logger.debug(
            "[$accountName] Finished payment for txId: $transactionId, " +
                    "payment: $paymentId, success: $success, reason: $reason"
        )
    }

    /**
     * Only records metrics — no DB, no semaphore release (for early-exit paths).
     */
    private fun recordFinish(
        success: Boolean,
        reason: String?,
        requestStartTime: Long,
        transactionId: UUID?,
        paymentId: UUID,
        releaseSemaphore: Boolean = true
    ) {
        val elapsed = now() - requestStartTime
        requestLatency.record(elapsed, TimeUnit.MILLISECONDS)
        logger.debug(
            "[$accountName] Finished payment for txId: $transactionId, " +
                    "payment: $paymentId, success: $success, reason: $reason"
        )
        if (releaseSemaphore) {
            semaphore.release()
        }
    }

    private sealed interface HttpResult {
        data class Completed(val response: ExternalSysResponse) : HttpResult
        data class Failed(val cause: Throwable) : HttpResult
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()