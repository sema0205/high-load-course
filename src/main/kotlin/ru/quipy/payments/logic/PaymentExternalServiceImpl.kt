package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
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

        // Настройки retry
        const val MAX_ATTEMPTS = 4
        const val RETRY_DELAY_MS = 1000L
        const val TEMPORARY_ERROR = "Temporary error"
    }

    private val requestCounter = Counter.builder("http_shop_payment_requests")
        .description("Total number of requests sent by shop to payment service")
        .register(meterRegistry)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

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

        // Retry loop: максимум 2 попытки
        for (attempt in 0 until MAX_ATTEMPTS) {
            // Проверяем deadline перед попыткой
            if (now() >= deadline) {
                logger.error("[$accountName] Deadline exceeded before attempt ${attempt + 1} for payment $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                }
                break
            }

            try {
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                        .header("deadline", deadline.toString())
                }.build()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt ${attempt + 1}, succeeded: ${body.result}, message: ${body.message}")

                    val requestDuration = System.currentTimeMillis() - requestStartTime
                    DistributionSummary.builder("payment_request_duration")
                        .description("Payment request duration with status code")
                        .tags("status_code", response.code.toString())
                        .publishPercentiles(0.5, 0.8, 0.99)
                        .register(meterRegistry)
                        .record(requestDuration.toDouble())

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    if (body.result) {
                        // Успех - завершаем
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                        break
                    } else if (body.message == TEMPORARY_ERROR && attempt < MAX_ATTEMPTS - 1) {
                        // Временная ошибка - retry если есть время
                        val timeForRetry = now() + RETRY_DELAY_MS + requestAverageProcessingTime.toMillis()
                        if (timeForRetry < deadline) {
                            logger.warn("[$accountName] Temporary error for payment $paymentId, retrying after $RETRY_DELAY_MS ms")
                            Thread.sleep(RETRY_DELAY_MS)
                            continue // следующая попытка
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
                            break
                        }
                    } else {
                        // Постоянная ошибка или последняя попытка
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = body.message)
                        }
                        break
                    }
                }

            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
                break // Не ретраим при исключениях
            }
        }

        // Освобождаем симафор
        semaphore.release()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()