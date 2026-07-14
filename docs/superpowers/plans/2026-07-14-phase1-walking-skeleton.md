# Phase 1 — Walking Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 1개 종목(KRW-BTC)의 실시간 체결이 `Upbit WebSocket 수집 → TimescaleDB 저장 → REST 조회`까지 끝까지 관통하고, 수집량이 측정되는 얇은 슬라이스를 완성한다.

**Architecture:** JDK 내장 `java.net.http.WebSocket`으로 Upbit에 연결해 체결 JSON을 수신 → Jackson으로 파싱 → `JdbcClient`로 멱등 INSERT(`ON CONFLICT DO NOTHING`) → 저장된 체결을 REST로 조회. 저장 경로는 ORM 임피던스(하이퍼테이블 복합 식별자·멱등 upsert)를 피하려 JPA 대신 `JdbcClient`를 쓴다. Micrometer 카운터로 수집/저장량을 측정한다.

**Tech Stack:** Kotlin 2.3 · Spring Boot 4.1 · Spring `JdbcClient` · Jackson 3(`tools.jackson.*`) · JDK HttpClient WebSocket · Micrometer(Actuator) · TimescaleDB(pg16, docker) · JUnit5(kotlin-test)

## Global Constraints

- 언어/런타임: **Kotlin (JVM 21)**, 빌드 Gradle(Kotlin DSL). (스펙 4·5절)
- 저장소: **TimescaleDB**, `trades` 하이퍼테이블. 스키마는 `db/init/01_init.sql`이 관리하며 Hibernate가 DDL을 만들지 않는다(`ddl-auto: validate`).
- 멱등성: 같은 `(code, time, sequential_id)`는 한 번만 저장(`uq_trades_dedup` 인덱스 + `ON CONFLICT DO NOTHING`). (스펙 3·DD 기재)
- Phase 1 종목: **KRW-BTC 1종목**. (스펙 DD-2)
- 측정을 처음부터 심는다: 수집/저장 카운터 + 주기적 처리량 로그. (스펙 DD-4)
- Jackson은 **버전 3** — ObjectMapper/kotlin 모듈은 `tools.jackson.*`, 애노테이션은 `com.fasterxml.jackson.annotation.*`.
- **선행 조건:** 개발/테스트 실행 전 `docker compose up -d`로 TimescaleDB가 떠 있어야 한다(저장 관련 테스트와 앱 부팅에 필요).
- 새 의존성 추가 없음 — 필요한 것(web, actuator, jdbc, jackson-kotlin)은 모두 스캐폴드에 포함됨.

**패키지 루트:** `com.candleforge`

---

## File Structure

**메인 코드 (`src/main/kotlin/com/candleforge/`)**
- `domain/Trade.kt` — 체결 도메인 모델(불변 data class). 모든 계층이 공유.
- `ingest/UpbitTradeParser.kt` — Upbit 체결 JSON → `Trade` 변환(+ 내부 DTO `UpbitTradeMessage`).
- `ingest/UpbitProperties.kt` — `candleforge.upbit.*` 설정 바인딩.
- `ingest/UpbitWebSocketClient.kt` — Upbit 연결·구독·바이너리 프레임 디코딩·재연결·하트비트. 저수준 커넥터.
- `ingest/TradeIngestionService.kt` — WS 클라이언트 ↔ 파서 ↔ 저장소 배선 + 측정 카운터.
- `storage/TradeRepository.kt` — 저장/조회 인터페이스.
- `storage/JdbcTradeRepository.kt` — `JdbcClient` 구현(멱등 insert, 최신순 조회).
- `api/TradeController.kt` — `GET /api/v1/trades` 조회 API(+ 응답 DTO).
- `config/AppConfig.kt` — `@EnableScheduling`, `@EnableConfigurationProperties`.

**테스트 (`src/test/kotlin/com/candleforge/`)**
- `ingest/UpbitTradeParserTest.kt` — 순수 단위(파싱).
- `storage/JdbcTradeRepositoryTest.kt` — 통합(로컬 TimescaleDB, `@Transactional` 롤백).
- `api/TradeControllerTest.kt` — `@WebMvcTest`(가짜 저장소).
- `ingest/UpbitWebSocketClientManualIT.kt` — 라이브 Upbit 수동 통합(기본 `@Disabled`).

**설정**
- `src/main/resources/application.yml` — `candleforge.upbit.*` 추가.
- `src/test/resources/application.yml` — 테스트 시 수집기 비활성화(`enabled: false`).

---

## Task 1: 도메인 모델 + Upbit 체결 파서

**Files:**
- Create: `src/main/kotlin/com/candleforge/domain/Trade.kt`
- Create: `src/main/kotlin/com/candleforge/ingest/UpbitTradeParser.kt`
- Test: `src/test/kotlin/com/candleforge/ingest/UpbitTradeParserTest.kt`

**Interfaces:**
- Produces:
  - `data class Trade(val time: Instant, val code: String, val price: BigDecimal, val volume: BigDecimal, val askBid: String, val sequentialId: Long)`
  - `class UpbitTradeParser(objectMapper: ObjectMapper) { fun parse(json: String): Trade }`

- [ ] **Step 1: 도메인 모델 작성** (`domain/Trade.kt`)

```kotlin
package com.candleforge.domain

import java.math.BigDecimal
import java.time.Instant

/** 단일 체결(trade) 한 건. 수집·저장·조회 전 계층이 공유하는 불변 모델. */
data class Trade(
    val time: Instant,
    val code: String,
    val price: BigDecimal,
    val volume: BigDecimal,
    val askBid: String,      // "ASK" 또는 "BID"
    val sequentialId: Long,  // Upbit 체결 고유 id (종목별 유일)
)
```

- [ ] **Step 2: 실패하는 파서 테스트 작성** (`ingest/UpbitTradeParserTest.kt`)

```kotlin
package com.candleforge.ingest

import tools.jackson.module.kotlin.jacksonObjectMapper
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class UpbitTradeParserTest {

    private val parser = UpbitTradeParser(jacksonObjectMapper())

    @Test
    fun `Upbit 체결 JSON을 Trade로 파싱한다`() {
        val json = """
            {"type":"trade","code":"KRW-BTC","timestamp":1700000000123,
             "trade_date":"2023-11-14","trade_time":"22:13:20",
             "trade_timestamp":1700000000000,"trade_price":50000000.0,
             "trade_volume":0.001,"ask_bid":"BID","prev_closing_price":49000000.0,
             "change":"RISE","change_price":1000000.0,
             "sequential_id":1700000000000001,"stream_type":"REALTIME"}
        """.trimIndent()

        val trade = parser.parse(json)

        assertEquals(Instant.ofEpochMilli(1700000000000), trade.time)
        assertEquals("KRW-BTC", trade.code)
        assertEquals(0, trade.price.compareTo(BigDecimal("50000000.0")))
        assertEquals(0, trade.volume.compareTo(BigDecimal("0.001")))
        assertEquals("BID", trade.askBid)
        assertEquals(1700000000000001L, trade.sequentialId)
    }
}
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `./gradlew test --tests "com.candleforge.ingest.UpbitTradeParserTest"`
Expected: FAIL — `UpbitTradeParser` 미정의(컴파일 에러).

- [ ] **Step 4: 파서 구현** (`ingest/UpbitTradeParser.kt`)

```kotlin
package com.candleforge.ingest

import com.candleforge.domain.Trade
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.readValue
import java.math.BigDecimal
import java.time.Instant

/** Upbit DEFAULT 포맷 체결 메시지의 필요한 필드만 매핑(나머지 무시). */
@JsonIgnoreProperties(ignoreUnknown = true)
data class UpbitTradeMessage(
    @param:JsonProperty("type") val type: String,
    @param:JsonProperty("code") val code: String,
    @param:JsonProperty("trade_price") val tradePrice: BigDecimal,
    @param:JsonProperty("trade_volume") val tradeVolume: BigDecimal,
    @param:JsonProperty("ask_bid") val askBid: String,
    @param:JsonProperty("sequential_id") val sequentialId: Long,
    @param:JsonProperty("trade_timestamp") val tradeTimestamp: Long,
)

@Component
class UpbitTradeParser(private val objectMapper: ObjectMapper) {

    /** 체결 JSON 문자열을 도메인 Trade로 변환. trade 타입이 아니면 예외. */
    fun parse(json: String): Trade {
        val msg: UpbitTradeMessage = objectMapper.readValue(json)
        require(msg.type == "trade") { "not a trade message: ${msg.type}" }
        return Trade(
            time = Instant.ofEpochMilli(msg.tradeTimestamp),
            code = msg.code,
            price = msg.tradePrice,
            volume = msg.tradeVolume,
            askBid = msg.askBid,
            sequentialId = msg.sequentialId,
        )
    }
}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `./gradlew test --tests "com.candleforge.ingest.UpbitTradeParserTest"`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/com/candleforge/domain/Trade.kt \
        src/main/kotlin/com/candleforge/ingest/UpbitTradeParser.kt \
        src/test/kotlin/com/candleforge/ingest/UpbitTradeParserTest.kt
git commit -m "feat: Trade 도메인 모델과 Upbit 체결 파서"
```

---

## Task 2: 저장소 (멱등 insert + 최신순 조회)

**Files:**
- Create: `src/main/kotlin/com/candleforge/storage/TradeRepository.kt`
- Create: `src/main/kotlin/com/candleforge/storage/JdbcTradeRepository.kt`
- Create: `src/test/resources/application.yml`
- Test: `src/test/kotlin/com/candleforge/storage/JdbcTradeRepositoryTest.kt`

**Interfaces:**
- Consumes: `Trade` (Task 1)
- Produces:
  - `interface TradeRepository { fun save(trade: Trade): Boolean; fun findRecent(code: String, limit: Int): List<Trade> }`
  - `class JdbcTradeRepository(jdbcClient: JdbcClient) : TradeRepository` — `save`는 새로 저장되면 `true`, 중복이면 `false`.

- [ ] **Step 1: 저장소 인터페이스 작성** (`storage/TradeRepository.kt`)

```kotlin
package com.candleforge.storage

import com.candleforge.domain.Trade

interface TradeRepository {
    /** 저장 성공(신규) 시 true, 멱등 충돌(중복)로 저장 안 되면 false. */
    fun save(trade: Trade): Boolean

    /** 특정 종목의 최신 체결을 시간 내림차순으로 limit개 조회. */
    fun findRecent(code: String, limit: Int): List<Trade>
}
```

- [ ] **Step 2: 테스트용 설정 작성** (`src/test/resources/application.yml`)

주의: 이 파일은 main의 `application.yml`을 **병합이 아니라 통째로 가린다**. 따라서 datasource 설정도 여기 포함해야 한다. 수집기는 테스트에서 끈다.

```yaml
spring:
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:candleforge}
    username: ${DB_USER:candleforge}
    password: ${DB_PASSWORD:candleforge}
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false

candleforge:
  upbit:
    enabled: false
    endpoint: wss://api.upbit.com/websocket/v1
    codes:
      - KRW-BTC
```

- [ ] **Step 3: 실패하는 저장소 테스트 작성** (`storage/JdbcTradeRepositoryTest.kt`)

`@Transactional`이라 각 테스트가 끝나면 롤백되어 DB가 더러워지지 않는다. (로컬 TimescaleDB가 떠 있어야 함)

```kotlin
package com.candleforge.storage

import com.candleforge.domain.Trade
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@SpringBootTest
@Transactional
class JdbcTradeRepositoryTest @Autowired constructor(
    private val repository: TradeRepository,
) {
    private fun sample(seqId: Long, price: String = "50000000") = Trade(
        time = Instant.ofEpochMilli(1700000000000),
        code = "KRW-BTC",
        price = BigDecimal(price),
        volume = BigDecimal("0.001"),
        askBid = "BID",
        sequentialId = seqId,
    )

    @Test
    fun `새 체결을 저장하면 true를 반환하고 조회된다`() {
        val saved = repository.save(sample(seqId = 1L))
        assertTrue(saved)

        val recent = repository.findRecent("KRW-BTC", 10)
        assertEquals(1, recent.size)
        assertEquals(1L, recent.first().sequentialId)
        assertEquals(0, recent.first().price.compareTo(BigDecimal("50000000")))
    }

    @Test
    fun `같은 (code,time,sequential_id) 중복 저장은 false를 반환한다`() {
        assertTrue(repository.save(sample(seqId = 2L)))
        assertFalse(repository.save(sample(seqId = 2L)))  // 멱등: 두 번째는 무시
    }

    @Test
    fun `findRecent는 시간 내림차순으로 반환한다`() {
        val older = sample(seqId = 10L).copy(time = Instant.ofEpochMilli(1000))
        val newer = sample(seqId = 11L).copy(time = Instant.ofEpochMilli(2000))
        repository.save(older)
        repository.save(newer)

        val recent = repository.findRecent("KRW-BTC", 10)
        assertEquals(11L, recent.first().sequentialId)  // 최신이 먼저
    }
}
```

- [ ] **Step 4: 테스트 실패 확인**

먼저 인프라 기동: `docker compose up -d`
Run: `./gradlew test --tests "com.candleforge.storage.JdbcTradeRepositoryTest"`
Expected: FAIL — `JdbcTradeRepository` 미구현(빈 주입 실패/컴파일 에러).

- [ ] **Step 5: 저장소 구현** (`storage/JdbcTradeRepository.kt`)

```kotlin
package com.candleforge.storage

import com.candleforge.domain.Trade
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime
import java.time.ZoneOffset

@Repository
class JdbcTradeRepository(private val jdbcClient: JdbcClient) : TradeRepository {

    override fun save(trade: Trade): Boolean {
        val updated = jdbcClient.sql(
            """
            INSERT INTO trades (time, code, price, volume, ask_bid, sequential_id)
            VALUES (:time, :code, :price, :volume, :askBid, :sequentialId)
            ON CONFLICT (code, "time", sequential_id) DO NOTHING
            """.trimIndent(),
        )
            .param("time", trade.time.atOffset(ZoneOffset.UTC))
            .param("code", trade.code)
            .param("price", trade.price)
            .param("volume", trade.volume)
            .param("askBid", trade.askBid)
            .param("sequentialId", trade.sequentialId)
            .update()
        return updated == 1
    }

    override fun findRecent(code: String, limit: Int): List<Trade> =
        jdbcClient.sql(
            """
            SELECT time, code, price, volume, ask_bid, sequential_id
            FROM trades
            WHERE code = :code
            ORDER BY time DESC
            LIMIT :limit
            """.trimIndent(),
        )
            .param("code", code)
            .param("limit", limit)
            .query { rs, _ ->
                Trade(
                    time = rs.getObject("time", OffsetDateTime::class.java).toInstant(),
                    code = rs.getString("code"),
                    price = rs.getBigDecimal("price"),
                    volume = rs.getBigDecimal("volume"),
                    askBid = rs.getString("ask_bid"),
                    sequentialId = rs.getLong("sequential_id"),
                )
            }
            .list()
}
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `./gradlew test --tests "com.candleforge.storage.JdbcTradeRepositoryTest"`
Expected: PASS (3개 테스트)

- [ ] **Step 7: 커밋**

```bash
git add src/main/kotlin/com/candleforge/storage/ \
        src/test/kotlin/com/candleforge/storage/JdbcTradeRepositoryTest.kt \
        src/test/resources/application.yml
git commit -m "feat: TradeRepository 멱등 저장/최신순 조회(JdbcClient)"
```

---

## Task 3: 조회 REST API

**Files:**
- Create: `src/main/kotlin/com/candleforge/api/TradeController.kt`
- Test: `src/test/kotlin/com/candleforge/api/TradeControllerTest.kt`

**Interfaces:**
- Consumes: `TradeRepository` (Task 2), `Trade` (Task 1)
- Produces: `GET /api/v1/trades?code={code}&limit={limit}` → JSON 배열. `limit` 기본 50, 최대 1000.

- [ ] **Step 1: 실패하는 컨트롤러 테스트 작성** (`api/TradeControllerTest.kt`)

`@WebMvcTest`는 웹 계층만 로드하므로 DB가 필요 없다. 저장소는 가짜 구현을 빈으로 주입.

```kotlin
package com.candleforge.api

import com.candleforge.domain.Trade
import com.candleforge.storage.TradeRepository
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest  // Boot 4 패키지
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.Test

@WebMvcTest(TradeController::class)
class TradeControllerTest(@org.springframework.beans.factory.annotation.Autowired val mockMvc: MockMvc) {

    @TestConfiguration
    class FakeRepoConfig {
        @Bean
        fun tradeRepository(): TradeRepository = object : TradeRepository {
            override fun save(trade: Trade) = true
            override fun findRecent(code: String, limit: Int): List<Trade> = listOf(
                Trade(Instant.ofEpochMilli(1700000000000), code,
                      BigDecimal("50000000"), BigDecimal("0.001"), "BID", 1L),
            )
        }
    }

    @Test
    fun `GET trades는 종목의 최신 체결을 반환한다`() {
        mockMvc.get("/api/v1/trades") {
            param("code", "KRW-BTC")
        }.andExpect {
            status { isOk() }
            jsonPath("$[0].code") { value("KRW-BTC") }
            jsonPath("$[0].sequentialId") { value(1) }
        }
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew test --tests "com.candleforge.api.TradeControllerTest"`
Expected: FAIL — `TradeController` 미정의.

- [ ] **Step 3: 컨트롤러 구현** (`api/TradeController.kt`)

```kotlin
package com.candleforge.api

import com.candleforge.domain.Trade
import com.candleforge.storage.TradeRepository
import java.math.BigDecimal
import java.time.Instant
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

data class TradeResponse(
    val time: Instant,
    val code: String,
    val price: BigDecimal,
    val volume: BigDecimal,
    val askBid: String,
    val sequentialId: Long,
) {
    companion object {
        fun from(t: Trade) = TradeResponse(t.time, t.code, t.price, t.volume, t.askBid, t.sequentialId)
    }
}

@RestController
@RequestMapping("/api/v1/trades")
class TradeController(private val tradeRepository: TradeRepository) {

    @GetMapping
    fun recent(
        @RequestParam code: String,
        @RequestParam(defaultValue = "50") limit: Int,
    ): List<TradeResponse> {
        val capped = limit.coerceIn(1, 1000)
        return tradeRepository.findRecent(code, capped).map(TradeResponse::from)
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests "com.candleforge.api.TradeControllerTest"`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/com/candleforge/api/TradeController.kt \
        src/test/kotlin/com/candleforge/api/TradeControllerTest.kt
git commit -m "feat: 체결 조회 REST API(GET /api/v1/trades)"
```

---

## Task 4: Upbit WebSocket 클라이언트 (연결·구독·디코딩·재연결)

**Files:**
- Create: `src/main/kotlin/com/candleforge/ingest/UpbitProperties.kt`
- Create: `src/main/kotlin/com/candleforge/ingest/UpbitWebSocketClient.kt`
- Create: `src/main/kotlin/com/candleforge/config/AppConfig.kt`
- Modify: `src/main/resources/application.yml` (candleforge.upbit.* 추가)
- Test: `src/test/kotlin/com/candleforge/ingest/UpbitWebSocketClientManualIT.kt` (수동, 기본 @Disabled)

**Interfaces:**
- Consumes: `UpbitProperties`
- Produces:
  - `data class UpbitProperties(val enabled: Boolean, val endpoint: String, val codes: List<String>)`
  - `class UpbitWebSocketClient(properties: UpbitProperties) { fun connect(onMessage: (String) -> Unit); fun close() }`
    - `onMessage`는 디코딩된 **JSON 문자열 1건**마다 호출된다.

- [ ] **Step 1: 설정 프로퍼티 작성** (`ingest/UpbitProperties.kt`)

```kotlin
package com.candleforge.ingest

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "candleforge.upbit")
data class UpbitProperties(
    val enabled: Boolean = true,
    val endpoint: String = "wss://api.upbit.com/websocket/v1",
    val codes: List<String> = listOf("KRW-BTC"),
)
```

- [ ] **Step 2: 설정 활성화 + 스케줄링** (`config/AppConfig.kt`)

```kotlin
package com.candleforge.config

import com.candleforge.ingest.UpbitProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@EnableScheduling
@EnableConfigurationProperties(UpbitProperties::class)
class AppConfig
```

- [ ] **Step 3: application.yml에 수집 설정 추가** (`src/main/resources/application.yml` 맨 아래에 추가)

```yaml

# 수집기 설정
candleforge:
  upbit:
    enabled: true
    endpoint: wss://api.upbit.com/websocket/v1
    codes:
      - KRW-BTC
```

- [ ] **Step 4: WebSocket 클라이언트 구현** (`ingest/UpbitWebSocketClient.kt`)

Upbit는 체결을 **바이너리 프레임(UTF-8 JSON)**으로 보낸다. 프레임을 누적했다가 `last==true`에 디코딩한다. 끊기면 지수 백오프로 재연결하고, 120초 idle 타임아웃을 피하려 주기적으로 "PING"을 보낸다.

```kotlin
package com.candleforge.ingest

import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean

class UpbitWebSocketClient(private val properties: UpbitProperties) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val httpClient: HttpClient = HttpClient.newHttpClient()
    private val running = AtomicBoolean(false)

    @Volatile private var webSocket: WebSocket? = null

    /** 연결을 시작하고, 디코딩된 JSON 메시지마다 onMessage를 호출한다. 논블로킹. */
    fun connect(onMessage: (String) -> Unit) {
        running.set(true)
        openWithRetry(onMessage, attempt = 0)
    }

    fun close() {
        running.set(false)
        webSocket?.sendClose(WebSocket.NORMAL_CLOSURE, "bye")
    }

    private fun openWithRetry(onMessage: (String) -> Unit, attempt: Int) {
        if (!running.get()) return
        httpClient.newWebSocketBuilder()
            .buildAsync(URI.create(properties.endpoint), Listener(onMessage))
            .whenComplete { ws, err ->
                if (err != null) {
                    val backoff = minOf(30_000L, 1_000L * (1L shl minOf(attempt, 5)))
                    log.warn("Upbit 연결 실패(attempt=$attempt), ${backoff}ms 후 재시도: ${err.message}")
                    Thread.sleep(backoff)
                    openWithRetry(onMessage, attempt + 1)
                } else {
                    webSocket = ws
                    subscribe(ws)
                    log.info("Upbit 연결 성공: codes=${properties.codes}")
                }
            }
    }

    /** 구독 요청 전송: [{ticket},{type:trade,codes},{format:DEFAULT}] */
    private fun subscribe(ws: WebSocket) {
        val codesJson = properties.codes.joinToString(",") { "\"$it\"" }
        val request = """[{"ticket":"${UUID.randomUUID()}"},{"type":"trade","codes":[$codesJson]},{"format":"DEFAULT"}]"""
        ws.sendText(request, true)
        ws.request(1)
    }

    private inner class Listener(private val onMessage: (String) -> Unit) : WebSocket.Listener {
        private val buffer = StringBuilder()

        override fun onBinary(ws: WebSocket, data: ByteBuffer, last: Boolean): CompletionStage<*>? {
            buffer.append(StandardCharsets.UTF_8.decode(data))
            if (last) {
                val json = buffer.toString()
                buffer.setLength(0)
                try { onMessage(json) } catch (e: Exception) { log.debug("메시지 처리 실패: ${e.message}") }
            }
            ws.request(1)
            return null
        }

        override fun onText(ws: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            // 일부 상태 메시지는 텍스트로 올 수 있음(UP 등) — trade가 아니면 무시됨
            try { onMessage(data.toString()) } catch (e: Exception) { log.debug("텍스트 무시: ${e.message}") }
            ws.request(1)
            return null
        }

        override fun onError(ws: WebSocket, error: Throwable) {
            log.warn("WebSocket 에러: ${error.message}")
            reconnect()
        }

        override fun onClose(ws: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
            log.warn("WebSocket 종료: $statusCode $reason")
            reconnect()
            return null
        }

        private fun reconnect() {
            if (running.get()) {
                log.info("재연결 시도")
                openWithRetry(onMessage, attempt = 0)
            }
        }
    }

    /** idle 타임아웃(120s) 회피용 하트비트. */
    fun ping() {
        webSocket?.sendText("PING", true)
    }
}
```

- [ ] **Step 5: 수동 통합 테스트 작성** (`ingest/UpbitWebSocketClientManualIT.kt`)

라이브 Upbit에 실제로 붙으므로 CI에서 자동 실행하지 않도록 `@Disabled`. 로컬에서 애노테이션만 잠깐 지우고 수동 확인한다.

```kotlin
package com.candleforge.ingest

import org.junit.jupiter.api.Disabled
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertTrue

@Disabled("수동 실행: 라이브 Upbit에 연결한다. 로컬에서만 확인.")
class UpbitWebSocketClientManualIT {

    @Test
    fun `실제 Upbit에서 최소 1건의 체결을 수신한다`() {
        val props = UpbitProperties(enabled = true,
            endpoint = "wss://api.upbit.com/websocket/v1", codes = listOf("KRW-BTC"))
        val client = UpbitWebSocketClient(props)
        val latch = CountDownLatch(1)

        client.connect { json ->
            println("수신: ${json.take(120)}")
            if (json.contains("\"type\":\"trade\"") || json.contains("\"trade\"")) latch.countDown()
        }

        val got = latch.await(15, TimeUnit.SECONDS)
        client.close()
        assertTrue(got, "15초 내 체결을 받지 못함")
    }
}
```

- [ ] **Step 6: 컴파일/기존 테스트 통과 확인**

Run: `./gradlew test`
Expected: PASS (Manual IT는 @Disabled로 스킵). 컴파일 에러 없어야 함.

- [ ] **Step 7: (수동) 라이브 수신 확인**

`UpbitWebSocketClientManualIT`의 `@Disabled`를 잠시 지우고:
Run: `./gradlew test --tests "com.candleforge.ingest.UpbitWebSocketClientManualIT"`
Expected: 콘솔에 "수신: ..." 로그가 찍히고 PASS. 확인 후 `@Disabled` 원복.

- [ ] **Step 8: 커밋**

```bash
git add src/main/kotlin/com/candleforge/ingest/UpbitProperties.kt \
        src/main/kotlin/com/candleforge/ingest/UpbitWebSocketClient.kt \
        src/main/kotlin/com/candleforge/config/AppConfig.kt \
        src/main/resources/application.yml \
        src/test/kotlin/com/candleforge/ingest/UpbitWebSocketClientManualIT.kt
git commit -m "feat: Upbit WebSocket 클라이언트(구독/디코딩/재연결/하트비트)"
```

---

## Task 5: 수집 배선 + 측정 (end-to-end)

**Files:**
- Create: `src/main/kotlin/com/candleforge/ingest/TradeIngestionService.kt`
- Test: (수동 end-to-end 확인 — 아래 Step 4)

**Interfaces:**
- Consumes: `UpbitWebSocketClient`, `UpbitTradeParser`, `TradeRepository`, `UpbitProperties`, `io.micrometer.core.instrument.MeterRegistry`
- Produces: 앱 기동 시(수집 enabled면) Upbit 연결 → 파싱 → 저장. 카운터 `candleforge.trades.received` / `candleforge.trades.stored`.

- [ ] **Step 1: 수집 서비스 구현** (`ingest/TradeIngestionService.kt`)

WS 클라이언트를 스프링 빈으로 만들고, 앱 준비 완료 시 수집을 시작한다. 파싱 실패(비-trade 메시지 등)는 조용히 무시한다.

```kotlin
package com.candleforge.ingest

import com.candleforge.storage.TradeRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import jakarta.annotation.PreDestroy

@Service
class TradeIngestionService(
    private val parser: UpbitTradeParser,
    private val repository: TradeRepository,
    private val properties: UpbitProperties,
    meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val client = UpbitWebSocketClient(properties)

    private val received: Counter = meterRegistry.counter("candleforge.trades.received")
    private val stored: Counter = meterRegistry.counter("candleforge.trades.stored")

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        if (!properties.enabled) {
            log.info("수집기 비활성화(candleforge.upbit.enabled=false)")
            return
        }
        log.info("Upbit 수집 시작: ${properties.codes}")
        client.connect(::handle)
    }

    private fun handle(json: String) {
        val trade = try {
            parser.parse(json)
        } catch (e: Exception) {
            return  // 비-trade/파싱불가 메시지는 무시
        }
        received.increment()
        if (repository.save(trade)) stored.increment()
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
```

- [ ] **Step 2: 전체 테스트 통과 확인 (수집기는 테스트에서 꺼짐)**

Run: `docker compose up -d && ./gradlew test`
Expected: PASS. 테스트 프로파일은 `enabled: false`라 라이브 연결 안 함.

- [ ] **Step 3: 앱 실행하여 end-to-end 확인**

```bash
docker compose up -d
./gradlew bootRun
```
Expected(수십 초 내 로그):
- `Upbit 수집 시작: [KRW-BTC]`
- `Upbit 연결 성공`
- 30초 후 `[측정] received=... stored=...` (received, stored 둘 다 증가)

- [ ] **Step 4: DB와 API로 저장 확인** (앱 실행 중, 새 터미널)

```bash
# DB에 실제로 쌓였는지
docker exec candleforge-timescaledb psql -U candleforge -d candleforge -tAc \
  "SELECT count(*), max(time) FROM trades WHERE code='KRW-BTC';"

# 조회 API
curl "http://localhost:8080/api/v1/trades?code=KRW-BTC&limit=5"

# 측정 지표
curl "http://localhost:8080/actuator/metrics/candleforge.trades.stored"
```
Expected: count > 0, API가 최근 체결 JSON 배열 반환, 지표 값 > 0. 확인 후 `bootRun` 종료(Ctrl+C).

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/com/candleforge/ingest/TradeIngestionService.kt
git commit -m "feat: 수집 배선(WS→파싱→저장) + 처리량 측정 카운터"
```

- [ ] **Step 6: Phase 1 완료 — README 갱신 후 커밋**

`README.md`의 "로컬 실행"을 실제 절차로 갱신:

```markdown
## 로컬 실행

1. 인프라 기동: `docker compose up -d` (TimescaleDB + Redis)
2. 앱 실행: `./gradlew bootRun`
3. 수집 확인: 로그의 `[측정] received/stored` 증가
4. 조회: `curl "http://localhost:8080/api/v1/trades?code=KRW-BTC&limit=5"`
```

```bash
git add README.md
git commit -m "docs: 로컬 실행 절차(Phase 1) README 반영"
git push
```

---

## Self-Review

**1. Spec coverage (스펙 8절 Phase 1 = "1종목 수집→저장→조회 관통 + 측정 하네스"):**
- 수집(Upbit WS) → Task 4. 저장(TimescaleDB) → Task 2. 조회(REST) → Task 3. 배선(end-to-end) → Task 5. 측정 하네스(카운터+로그+actuator) → Task 5. 멱등성 → Task 2(`ON CONFLICT`). 재연결 → Task 4. 모든 Phase 1 요구가 태스크에 매핑됨. ✔

**2. Placeholder scan:** "TBD/적절히 처리" 류 없음. 모든 코드/명령/기대출력 명시. ✔

**3. Type consistency:** `Trade`(6필드) 전 태스크 동일. `TradeRepository.save(Trade): Boolean` / `findRecent(String, Int): List<Trade>` — Task2 정의, Task3·5 동일 사용. `UpbitWebSocketClient.connect((String)->Unit)` — Task4 정의, Task5 사용 일치. `UpbitProperties(enabled, endpoint, codes)` — Task4 정의, Task2 테스트yml·Task5 사용 일치. ✔

**주의(실행 시):**
- 저장 관련 테스트/실행 전 반드시 `docker compose up -d`.
- Jackson 3 import 경로(`tools.jackson.*`) — Task1에서 처음 컴파일 시 확인됨.
- Task4 Step7·Task5 Step3~4는 라이브 Upbit 대상 **수동** 검증(자동 CI 아님).
