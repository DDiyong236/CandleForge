package com.candleforge.ingest

import com.candleforge.domain.Trade
import com.candleforge.storage.TradeRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import java.util.concurrent.ConcurrentLinkedQueue
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class TradeIngestionService(
    private val parser: UpbitTradeParser,
    private val repository: TradeRepository,
    private val properties: UpbitProperties,
    private val marketClient: UpbitMarketClient,
    meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val client = UpbitWebSocketClient(properties)

    private val received: Counter = meterRegistry.counter("candleforge.trades.received")
    private val stored: Counter = meterRegistry.counter("candleforge.trades.stored")

    // WS 스레드가 add, 스케줄러 스레드가 poll → 여러 스레드가 공유하므로 동시성 안전 큐 사용
    private val buffer = ConcurrentLinkedQueue<Trade>()

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        if (!properties.enabled) {
            log.info("수집기 비활성화(candleforge.upbit.enabled=false)")
            return
        }
        val codes = resolveCodes()
        log.info("Upbit 수집 시작: ${codes.size}종목")
        client.connect(codes, ::handle)
    }

    private fun resolveCodes(): List<String> =
        if (properties.dynamicCodes) {
            try {
                marketClient.fetchKrwMarkets()
            } catch (e: Exception) {
                log.warn("종목 목록 조회 실패, 설정값으로 대체: ${e.message}")
                properties.codes
            }
        } else {
            properties.codes
        }

    private fun handle(json: String) {
        val trade = try {
            parser.parse(json)
        } catch (e: Exception) {
            return  // 비-trade/파싱불가 메시지는 무시
        }
        received.increment()
        buffer.add(trade)   // 즉시 저장하지 않고 버퍼에 모음
    }

    /** 0.5초마다 버퍼를 비워 배치 저장. */
    @Scheduled(fixedRate = 500)
    fun flush() {
        if (buffer.isEmpty()) return
        val batch = ArrayList<Trade>()
        while (true) {
            val t = buffer.poll() ?: break
            batch.add(t)
        }
        if (batch.isEmpty()) return
        val inserted = repository.saveAll(batch)
        if (inserted > 0) stored.increment(inserted.toDouble())
    }

    /** idle 타임아웃 회피 + 30초마다 처리량 로그(측정). */
    @Scheduled(fixedRate = 30_000)
    fun heartbeatAndReport() {
        if (!properties.enabled) return
        client.ping()
        log.info("[측정] received=${received.count().toLong()} stored=${stored.count().toLong()}")
    }

    @PreDestroy
    fun stop() = client.close()
}
