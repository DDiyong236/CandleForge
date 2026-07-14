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
