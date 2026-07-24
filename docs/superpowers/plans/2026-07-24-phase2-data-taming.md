# Phase 2 — 데이터 길들이기 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 전체 KRW 마켓(~200종목)을 수집·배치 저장하고, TimescaleDB 연속집계로 1분봉을 자동 생성하며, 원본은 24시간 뒤 삭제·캔들은 압축해 DB 크기를 통제하고, 이를 측정으로 증명한다.

**Architecture:** 앱은 Upbit REST로 KRW 종목 목록을 받아 다중 구독하고, 수신 체결을 버퍼에 모아 배치 INSERT한다. TimescaleDB가 연속집계(1분봉)·보관정책(원본 24h)·압축(캔들 7일)을 자동 수행한다. 앱은 수집·유입만, 수명주기는 DB가 담당.

**Tech Stack:** Kotlin 2.3 · Spring Boot 4.1 · JDK HttpClient(REST/WS) · Jackson 3(`tools.jackson.*`) · Spring `JdbcTemplate`(배치)·`JdbcClient` · TimescaleDB 2.28(연속집계·보관·압축) · Micrometer

## Global Constraints

- 언어/런타임: **Kotlin (JVM 21)**, Gradle(Kotlin DSL). 새 의존성 추가 없음(web·jdbc·jackson 이미 포함).
- 종목: **전체 KRW 마켓**, Upbit `GET /v1/market/all`에서 `market`이 `"KRW-"`로 시작하는 것. (rate limit 10/s — 시작 시 1회만 호출)
- 캔들: **1분봉만**(`candles_1m`). OHLCV = `first/max/min/last/sum`.
- 보관/갱신 불변식: **`start_offset(1h) < drop_after(24h)`**. 위반 시 캔들 손실.
  - 갱신: start_offset=1h, end_offset=1분, schedule_interval=1분.
  - 보관: 원본 `trades` drop_after=24h.
- 압축: **캔들만**, `compress_after=7 days`. 원본은 압축 안 함.
- 측정: `pg_total_relation_size`/`hypertable_compression_stats`로 크기·처리량 before/after 기록.
- Jackson 3: ObjectMapper=`tools.jackson.databind`, 애노테이션=`com.fasterxml.jackson.annotation`.
- **선행 조건:** 각 태스크 전 `docker compose up -d`. Task 4·5의 스키마는 기존 DB에 자동 적용 안 되므로 **`docker compose down -v && docker compose up -d`로 볼륨 초기화 후 재적용**(01·02 init 재실행).

**패키지 루트:** `com.candleforge`

---

## File Structure

**메인 (`src/main/kotlin/com/candleforge/`)**
- `ingest/UpbitMarketClient.kt` — 신규. Upbit REST로 KRW 종목 목록 조회(+파싱).
- `ingest/UpbitProperties.kt` — 변경. `dynamicCodes` 플래그 추가.
- `ingest/UpbitWebSocketClient.kt` — 변경. `connect(codes, onMessage)`로 다중 종목 구독.
- `ingest/TradeIngestionService.kt` — 변경. 시작 시 종목 조회, 버퍼링+배치 flush.
- `storage/TradeRepository.kt` — 변경. `saveAll` 추가.
- `storage/JdbcTradeRepository.kt` — 변경. `JdbcTemplate` 배치 구현.

**DB (`db/init/`)**
- `02_candles.sql` — 신규. 연속집계·갱신·보관·압축 정책.

**테스트 (`src/test/kotlin/com/candleforge/`)**
- `ingest/UpbitMarketClientTest.kt` — 신규. KRW 필터 파싱(단위).
- `storage/JdbcTradeRepositoryTest.kt` — 변경. `saveAll` 통합 테스트 추가.
- `api/TradeControllerTest.kt` — 변경. 가짜 저장소에 `saveAll` 구현 추가.
- `ingest/UpbitWebSocketClientManualIT.kt` — 변경. `connect(codes,...)` 시그니처 반영.

---

## Task 1: Upbit 종목 목록 조회 (REST)

**Files:**
- Create: `src/main/kotlin/com/candleforge/ingest/UpbitMarketClient.kt`
- Test: `src/test/kotlin/com/candleforge/ingest/UpbitMarketClientTest.kt`

**Interfaces:**
- Consumes: `tools.jackson.databind.ObjectMapper` (빈)
- Produces:
  - `class UpbitMarketClient(objectMapper) { fun fetchKrwMarkets(): List<String>; fun parseKrwMarkets(json: String): List<String> }`
  - `parseKrwMarkets`는 `market`이 `"KRW-"`로 시작하는 코드만 반환.

- [ ] **Step 1: 실패 테스트 작성** (`ingest/UpbitMarketClientTest.kt`)

```kotlin
package com.candleforge.ingest

import tools.jackson.module.kotlin.jacksonObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals

class UpbitMarketClientTest {
    private val client = UpbitMarketClient(jacksonObjectMapper())

    @Test
    fun `KRW 마켓 코드만 걸러낸다`() {
        val json = """
            [
              {"market":"KRW-BTC","korean_name":"비트코인","english_name":"Bitcoin"},
              {"market":"BTC-ETH","korean_name":"이더리움","english_name":"Ethereum"},
              {"market":"KRW-ETH","korean_name":"이더리움","english_name":"Ethereum"},
              {"market":"USDT-BTC","korean_name":"비트코인","english_name":"Bitcoin"}
            ]
        """.trimIndent()

        val codes = client.parseKrwMarkets(json)

        assertEquals(listOf("KRW-BTC", "KRW-ETH"), codes)
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew test --tests "com.candleforge.ingest.UpbitMarketClientTest"`
Expected: FAIL — `UpbitMarketClient` 미정의.

- [ ] **Step 3: 구현** (`ingest/UpbitMarketClient.kt`)

```kotlin
package com.candleforge.ingest

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.readValue
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@JsonIgnoreProperties(ignoreUnknown = true)
data class UpbitMarket(
    @param:JsonProperty("market") val market: String,
)

@Component
class UpbitMarketClient(private val objectMapper: ObjectMapper) {

    private val httpClient: HttpClient = HttpClient.newHttpClient()

    /** Upbit에서 거래 가능한 전체 마켓을 받아 KRW 마켓 코드만 반환. */
    fun fetchKrwMarkets(): List<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("https://api.upbit.com/v1/market/all"))
            .GET()
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        return parseKrwMarkets(response.body())
    }

    /** 마켓 목록 JSON에서 code가 "KRW-"로 시작하는 것만 추출. */
    fun parseKrwMarkets(json: String): List<String> {
        val markets: List<UpbitMarket> = objectMapper.readValue(json)
        return markets.map { it.market }.filter { it.startsWith("KRW-") }
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew test --tests "com.candleforge.ingest.UpbitMarketClientTest"`
Expected: PASS

- [ ] **Step 5: (수동) 실제 조회 확인** — 실제 Upbit 호출로 종목 수 확인

Run: `curl -s "https://api.upbit.com/v1/market/all" | grep -o '"KRW-[A-Z0-9]*"' | sort -u | wc -l`
Expected: 150~200 사이 숫자 (KRW 종목 수). 이 값이 곧 다중 구독 대상 규모.

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/com/candleforge/ingest/UpbitMarketClient.kt \
        src/test/kotlin/com/candleforge/ingest/UpbitMarketClientTest.kt
git commit -m "feat: Upbit KRW 마켓 목록 조회(REST)"
```

---

## Task 2: 다중 종목 구독

**Files:**
- Modify: `src/main/kotlin/com/candleforge/ingest/UpbitProperties.kt`
- Modify: `src/main/kotlin/com/candleforge/ingest/UpbitWebSocketClient.kt`
- Modify: `src/main/kotlin/com/candleforge/ingest/TradeIngestionService.kt`
- Modify: `src/main/resources/application.yml`
- Modify: `src/test/kotlin/com/candleforge/ingest/UpbitWebSocketClientManualIT.kt`

**Interfaces:**
- Consumes: `UpbitMarketClient.fetchKrwMarkets()` (Task 1)
- Produces:
  - `UpbitProperties(enabled, endpoint, codes, dynamicCodes)` — `dynamicCodes: Boolean = true`
  - `UpbitWebSocketClient.connect(codes: List<String>, onMessage: (String) -> Unit)` — 구독 종목을 인자로 받음

- [ ] **Step 1: 프로퍼티에 dynamicCodes 추가** (`ingest/UpbitProperties.kt` 전체 교체)

```kotlin
package com.candleforge.ingest

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "candleforge.upbit")
data class UpbitProperties(
    val enabled: Boolean = true,
    val endpoint: String = "wss://api.upbit.com/websocket/v1",
    val codes: List<String> = listOf("KRW-BTC"),
    val dynamicCodes: Boolean = true,  // true면 전체 KRW 마켓을 REST로 조회해 구독
)
```

- [ ] **Step 2: 클라이언트가 codes를 인자로 받도록 변경** (`ingest/UpbitWebSocketClient.kt`)

`connect` 시그니처와 `subscribe`를 수정한다. 아래 두 부분만 교체:

기존:
```kotlin
    fun connect(onMessage: (String) -> Unit) {
        running.set(true)
        openWithRetry(onMessage, attempt = 0)
    }
```
교체:
```kotlin
    @Volatile private var subscribedCodes: List<String> = emptyList()

    fun connect(codes: List<String>, onMessage: (String) -> Unit) {
        subscribedCodes = codes
        running.set(true)
        openWithRetry(onMessage, attempt = 0)
    }
```

기존:
```kotlin
    private fun subscribe(ws: WebSocket) {
        val codesJson = properties.codes.joinToString(",") { "\"$it\"" }
```
교체:
```kotlin
    private fun subscribe(ws: WebSocket) {
        val codesJson = subscribedCodes.joinToString(",") { "\"$it\"" }
```

(`@Volatile private var subscribedCodes`는 클래스 상단 `webSocket` 선언 옆에 두어도 되고, 위처럼 connect 앞에 두어도 됨. 한 번만 선언할 것.)

- [ ] **Step 3: 서비스가 종목을 조회해 넘기도록 변경** (`ingest/TradeIngestionService.kt`)

생성자에 `UpbitMarketClient` 주입, `start()`에서 종목 결정:

기존 생성자 파라미터에 추가:
```kotlin
    private val marketClient: UpbitMarketClient,
```

기존 `start()` 교체:
```kotlin
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
```

- [ ] **Step 4: application.yml에 dynamicCodes 추가** (`src/main/resources/application.yml`의 `candleforge.upbit` 블록)

기존:
```yaml
candleforge:
  upbit:
    enabled: true
    endpoint: wss://api.upbit.com/websocket/v1
    codes:
      - KRW-BTC
```
교체:
```yaml
candleforge:
  upbit:
    enabled: true
    endpoint: wss://api.upbit.com/websocket/v1
    dynamicCodes: true      # 전체 KRW 마켓 자동 구독
    codes:                  # dynamicCodes=false일 때만 사용
      - KRW-BTC
```

- [ ] **Step 5: 수동 IT 시그니처 수정** (`ingest/UpbitWebSocketClientManualIT.kt`)

기존:
```kotlin
        client.connect { json ->
```
교체:
```kotlin
        client.connect(listOf("KRW-BTC")) { json ->
```

- [ ] **Step 6: 전체 테스트 통과 확인 (컴파일)**

Run: `docker compose up -d && ./gradlew test`
Expected: PASS. (테스트는 `enabled: false`라 라이브 연결·조회 안 함)

- [ ] **Step 7: (수동) 다중 구독 end-to-end 확인**

Run: `./gradlew bootRun` (별도 터미널에서 아래 확인)
```bash
docker exec candleforge-timescaledb psql -U candleforge -d candleforge -tAc \
  "SELECT count(DISTINCT code) FROM trades;"
```
Expected: 로그에 `Upbit 수집 시작: NNN종목`(수십~수백), 그리고 distinct code가 여러 개(다중 종목 저장 확인). 확인 후 Ctrl+C.

- [ ] **Step 8: 커밋**

```bash
git add src/main/kotlin/com/candleforge/ingest/ src/main/resources/application.yml \
        src/test/kotlin/com/candleforge/ingest/UpbitWebSocketClientManualIT.kt
git commit -m "feat: 전체 KRW 마켓 다중 종목 구독"
```

---

## Task 3: 배치 insert (저장 최적화)

**Files:**
- Modify: `src/main/kotlin/com/candleforge/storage/TradeRepository.kt`
- Modify: `src/main/kotlin/com/candleforge/storage/JdbcTradeRepository.kt`
- Modify: `src/main/kotlin/com/candleforge/ingest/TradeIngestionService.kt`
- Modify: `src/test/kotlin/com/candleforge/storage/JdbcTradeRepositoryTest.kt`
- Modify: `src/test/kotlin/com/candleforge/api/TradeControllerTest.kt`

**Interfaces:**
- Produces: `TradeRepository.saveAll(trades: List<Trade>): Int` — 새로 저장된(중복 아닌) 건수 반환.

- [ ] **Step 1: 인터페이스에 saveAll 추가** (`storage/TradeRepository.kt`)

기존 인터페이스 본문에 한 줄 추가:
```kotlin
interface TradeRepository {
    fun save(trade: Trade): Boolean
    fun saveAll(trades: List<Trade>): Int          // 배치 저장, 신규 건수 반환
    fun findRecent(code: String, limit: Int): List<Trade>
}
```

- [ ] **Step 2: 가짜 저장소에 saveAll 구현 추가** (`api/TradeControllerTest.kt`의 `object : TradeRepository`)

기존 `override fun save(...)` 아래에 추가:
```kotlin
            override fun saveAll(trades: List<Trade>) = trades.size
```

- [ ] **Step 3: 실패 테스트 작성** (`storage/JdbcTradeRepositoryTest.kt`에 메서드 추가)

기존 클래스 안에 테스트 추가(기존 `sample` 헬퍼 재사용):
```kotlin
    @Test
    fun `saveAll은 여러 건을 저장하고 신규 건수를 반환한다`() {
        val batch = listOf(sample(seqId = 100L), sample(seqId = 101L), sample(seqId = 102L))
        val inserted = repository.saveAll(batch)
        assertEquals(3, inserted)

        val recent = repository.findRecent("TEST-BTC", 10)
        assertEquals(3, recent.size)
    }

    @Test
    fun `saveAll은 중복을 제외한 신규만 센다`() {
        repository.save(sample(seqId = 200L))                       // 먼저 1건 저장
        val batch = listOf(sample(seqId = 200L), sample(seqId = 201L))  // 200은 중복
        val inserted = repository.saveAll(batch)
        assertEquals(1, inserted)                                   // 201만 신규
    }
```

- [ ] **Step 4: 실패 확인**

Run: `docker compose up -d && ./gradlew test --tests "com.candleforge.storage.JdbcTradeRepositoryTest"`
Expected: FAIL — `saveAll` 미구현(컴파일 에러).

- [ ] **Step 5: JdbcTemplate 배치 구현** (`storage/JdbcTradeRepository.kt`)

생성자에 `JdbcTemplate` 주입, `saveAll` 추가. import 추가.

기존 생성자:
```kotlin
@Repository
class JdbcTradeRepository(private val jdbcClient: JdbcClient) : TradeRepository {
```
교체:
```kotlin
@Repository
class JdbcTradeRepository(
    private val jdbcClient: JdbcClient,
    private val jdbcTemplate: JdbcTemplate,
) : TradeRepository {
```

파일 상단 import에 추가:
```kotlin
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.PreparedStatement
```

클래스 안(save와 findRecent 사이 아무 곳)에 추가:
```kotlin
    override fun saveAll(trades: List<Trade>): Int {
        if (trades.isEmpty()) return 0
        val sql = """
            INSERT INTO trades (time, code, price, volume, ask_bid, sequential_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (code, "time", sequential_id) DO NOTHING
        """.trimIndent()
        val results = jdbcTemplate.batchUpdate(sql, object : BatchPreparedStatementSetter {
            override fun setValues(ps: PreparedStatement, i: Int) {
                val t = trades[i]
                ps.setObject(1, t.time.atOffset(ZoneOffset.UTC))
                ps.setString(2, t.code)
                ps.setBigDecimal(3, t.price)
                ps.setBigDecimal(4, t.volume)
                ps.setString(5, t.askBid)
                ps.setLong(6, t.sequentialId)
            }
            override fun getBatchSize(): Int = trades.size
        })
        return results.count { it > 0 }   // 신규 삽입된 행만(중복은 0)
    }
```

- [ ] **Step 6: 통과 확인**

Run: `./gradlew test --tests "com.candleforge.storage.JdbcTradeRepositoryTest"`
Expected: PASS (기존 3 + 신규 2 = 5개)

- [ ] **Step 7: 수집 서비스를 버퍼링+배치 flush로 전환** (`ingest/TradeIngestionService.kt`)

`handle`이 즉시 저장하지 않고 버퍼에 쌓고, 주기적으로 배치 저장한다.

파일 상단 import 추가:
```kotlin
import java.util.concurrent.ConcurrentLinkedQueue
```

클래스 필드에 버퍼 추가(카운터 선언 옆):
```kotlin
    private val buffer = ConcurrentLinkedQueue<com.candleforge.domain.Trade>()
```

기존 `handle` 교체:
```kotlin
    private fun handle(json: String) {
        val trade = try {
            parser.parse(json)
        } catch (e: Exception) {
            return
        }
        received.increment()
        buffer.add(trade)          // 즉시 저장하지 않고 버퍼에 모음
    }

    /** 0.5초마다 버퍼를 비워 배치 저장. */
    @Scheduled(fixedRate = 500)
    fun flush() {
        if (buffer.isEmpty()) return
        val batch = ArrayList<com.candleforge.domain.Trade>()
        while (true) {
            val t = buffer.poll() ?: break
            batch.add(t)
        }
        if (batch.isEmpty()) return
        val inserted = repository.saveAll(batch)
        if (inserted > 0) stored.increment(inserted.toDouble())
    }
```

(기존 `heartbeatAndReport`의 `@Scheduled(fixedRate = 30_000)`는 그대로 두고 ping+측정 로그 유지.)

- [ ] **Step 8: 전체 테스트 통과 확인**

Run: `./gradlew test`
Expected: PASS.

- [ ] **Step 9: (측정) 배치 처리량 확인** — README용 숫자

`./gradlew bootRun` 후 30초 측정 로그의 `received`/`stored` 증가폭을 기록(배치 후). Phase 1의 단건 대비 처리량을 README에 적기 위한 after 값.
Expected: 200종목이면 30초에 received 수백~수천 증가. Ctrl+C로 종료.

- [ ] **Step 10: 커밋**

```bash
git add src/main/kotlin/com/candleforge/storage/ src/main/kotlin/com/candleforge/ingest/TradeIngestionService.kt \
        src/test/kotlin/com/candleforge/storage/JdbcTradeRepositoryTest.kt \
        src/test/kotlin/com/candleforge/api/TradeControllerTest.kt
git commit -m "feat: 배치 insert(saveAll) + 버퍼링 flush로 저장 최적화"
```

---

## Task 4: 연속집계 (1분봉 자동 생성)

**Files:**
- Create: `db/init/02_candles.sql`

**Interfaces:**
- Produces: `candles_1m` 연속집계 뷰 (bucket, code, open, high, low, close, volume) + 1분 갱신 정책.

- [ ] **Step 1: 연속집계 SQL 작성** (`db/init/02_candles.sql`)

```sql
-- Phase 2: 1분봉 연속집계 (TimescaleDB 최초 기동 시 실행. 기존 DB엔 수동/볼륨초기화로 적용)

-- 1) 1분봉 연속집계 정의 (OHLCV)
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    code,
    first(price, time) AS open,    -- 시간순 첫 가격 = 시가
    max(price)         AS high,    -- 고가
    min(price)         AS low,     -- 저가
    last(price, time)  AS close,   -- 시간순 마지막 가격 = 종가
    sum(volume)        AS volume   -- 거래량 합
FROM trades
GROUP BY bucket, code
WITH NO DATA;

-- 2) 갱신 정책: 1분마다, 최근 1시간 구간을, 1분 전까지 갱신
--    start_offset(1h) < 보관기간(24h) 이므로 삭제 영역을 건드리지 않음
SELECT add_continuous_aggregate_policy('candles_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');
```

- [ ] **Step 2: 볼륨 초기화 후 재적용** (init 스크립트는 빈 DB에서만 실행되므로)

```bash
cd /c/CandleForge
docker compose down -v          # 기존 데이터·볼륨 삭제
docker compose up -d            # 01_init.sql + 02_candles.sql 재실행
```
Expected: 두 컨테이너 healthy.

- [ ] **Step 3: 연속집계 생성 확인**

```bash
docker exec candleforge-timescaledb psql -U candleforge -d candleforge -c \
  "SELECT view_name FROM timescaledb_information.continuous_aggregates;"
```
Expected: `candles_1m` 한 줄.

- [ ] **Step 4: 데이터 흘려서 캔들 생성 확인** — 앱을 몇 분 돌린다

```bash
./gradlew bootRun    # 최소 2~3분 실행 (갱신 1분 주기 + 정지선 1분이라 여유 두기), 이후 Ctrl+C
```
그 다음:
```bash
docker exec candleforge-timescaledb psql -U candleforge -d candleforge -c \
  "SELECT code, bucket, open, high, low, close, volume FROM candles_1m ORDER BY bucket DESC LIMIT 5;"
```
Expected: 1분봉 여러 행. open/high/low/close/volume이 채워져 있어야 함(open=구간 첫가, close=마지막가).

- [ ] **Step 5: 커밋**

```bash
git add db/init/02_candles.sql
git commit -m "feat: TimescaleDB 1분봉 연속집계 + 갱신 정책"
```

---

## Task 5: 보관정책 + 압축 + 측정

**Files:**
- Modify: `db/init/02_candles.sql` (정책 추가)
- Create: `db/measure.sql` (측정 쿼리 모음)

**Interfaces:**
- Produces: 원본 24h 보관정책, 캔들 7일 압축정책, 크기 측정 쿼리.

- [ ] **Step 1: 보관·압축 정책을 02_candles.sql에 추가** (파일 끝에 append)

```sql

-- 3) 원본 보관정책: trades를 24시간 뒤 자동 삭제
--    start_offset(1h) < 24h 이므로 갱신이 삭제 영역을 넘겨다보지 않음(캔들 무결성 보장)
SELECT add_retention_policy('trades', drop_after => INTERVAL '24 hours');

-- 4) 캔들 압축: 7일 지난 candles_1m 청크를 압축(무손실, 장기보관용)
--    (TimescaleDB 2.28 — compress 플래그 후 압축정책. 버전에 따라 columnstore 명칭일 수 있음)
ALTER MATERIALIZED VIEW candles_1m SET (timescaledb.compress = true);
SELECT add_compression_policy('candles_1m', compress_after => INTERVAL '7 days');
```

- [ ] **Step 2: 측정 쿼리 작성** (`db/measure.sql`)

```sql
-- CandleForge DB 크기 측정 (before/after 기록용)
-- 실행: docker exec candleforge-timescaledb psql -U candleforge -d candleforge -f /...  또는 -c 로 개별 실행

-- 원본/캔들 총 크기
SELECT 'trades'     AS name, pg_size_pretty(hypertable_size('trades'))     AS size
UNION ALL
SELECT 'candles_1m' AS name, pg_size_pretty(hypertable_size('candles_1m')) AS size;

-- 행 수
SELECT 'trades' AS name, count(*) FROM trades
UNION ALL
SELECT 'candles_1m' AS name, count(*) FROM candles_1m;

-- 캔들 압축 효과(압축된 청크가 생긴 뒤 의미 있음)
SELECT * FROM hypertable_compression_stats('candles_1m');
```

- [ ] **Step 3: 볼륨 초기화 후 정책 적용**

```bash
docker compose down -v && docker compose up -d
docker exec candleforge-timescaledb psql -U candleforge -d candleforge -c \
  "SELECT hypertable_name, proc_name FROM timescaledb_information.jobs WHERE proc_name LIKE '%retention%' OR proc_name LIKE '%compression%' OR proc_name LIKE '%refresh%';"
```
Expected: refresh(연속집계)·retention·compression 관련 job이 등록되어 나열됨.
(만약 `add_compression_policy`/`timescaledb.compress`가 2.28에서 에러면, 에러 메시지의 신 문법(columnstore)으로 교체 후 재적용 — 실행 시 확인.)

- [ ] **Step 4: 크기 측정 (before 스냅샷)** — 앱을 일정 시간 돌린 직후

```bash
./gradlew bootRun    # 5~10분 정도 데이터 축적 후 Ctrl+C
docker exec candleforge-timescaledb psql -U candleforge -d candleforge -f /dev/stdin < db/measure.sql
```
Expected: trades·candles_1m 크기와 행 수 출력. **이 수치를 README에 "N분간 원본 X MB, 캔들 Y MB"로 기록.** (24h·7일 정책의 완전한 효과는 장시간 후에 나타나므로, 초기엔 추세와 비율로 서술.)

- [ ] **Step 5: README에 Phase 2 측정 결과 정리** (`README.md`)

`## 진행 상황` 위 또는 로컬 실행 섹션 뒤에 측정 요약 추가(실측값으로 채움):

```markdown
## 데이터 길들이기 (Phase 2)

- 전체 KRW 마켓(~N종목) 실시간 수집, 배치 insert
- 1분봉 연속집계 자동 생성, 원본 24h 보관 후 삭제, 캔들 7일 후 압축
- 측정(실측): 원본 X분간 A MB / 캔들 B MB / 압축률 약 C배
- 방치 시 추정 월 D GB → 정책 적용 후 수 GB로 통제
```

- [ ] **Step 6: 커밋**

```bash
git add db/init/02_candles.sql db/measure.sql README.md
git commit -m "feat: 원본 보관정책(24h) + 캔들 압축(7일) + DB 크기 측정"
```

---

## Self-Review

**1. Spec coverage (2026-07-24-phase2 스펙):**
- DD-1 전체 KRW 종목 → Task 1(조회)+Task 2(구독). DD-2 연속집계 → Task 4. DD-3 1분봉만 → Task 4(candles_1m 하나). DD-4 보관/갱신(start<drop) → Task 4(갱신)+Task 5(보관). DD-5 캔들 압축 7일 → Task 5. DD-6 배치 insert → Task 3. DD-7 측정 → Task 5(measure.sql, README). 전부 매핑됨. ✔

**2. Placeholder scan:** "TBD/적절히" 없음. 모든 코드·SQL·명령 명시. 측정값은 실측으로 채우도록 지시(플레이스홀더 아님). ✔

**3. Type consistency:** `saveAll(List<Trade>): Int` — Task 3에서 인터페이스·구현·가짜·서비스 일관. `connect(codes: List<String>, onMessage)` — Task 2에서 클라이언트·서비스·수동IT 일관. `parseKrwMarkets(String): List<String>` / `fetchKrwMarkets(): List<String>` — Task 1 정의, Task 2 사용 일치. `UpbitProperties(...dynamicCodes)` — Task 2 정의·yml·서비스 일치. ✔

**주의(실행 시):**
- 모든 DB 작업 전 `docker compose up -d`. Task 4·5는 스키마 변경이라 **`down -v && up`으로 재적용**.
- TimescaleDB 2.28 압축 문법(`timescaledb.compress`/`add_compression_policy`)이 버전에 따라 columnstore 명칭일 수 있음 — Task 5 Step 3에서 에러 시 신 문법으로 교체.
- Task 1 Step 5, Task 2 Step 7, Task 3 Step 9, Task 4 Step 4, Task 5 Step 4는 라이브 Upbit/실측 대상 **수동** 검증.
