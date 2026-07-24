package com.candleforge.api

import com.candleforge.domain.Trade
import com.candleforge.storage.TradeRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.Test

@WebMvcTest(TradeController::class)
class TradeControllerTest(@Autowired val mockMvc: MockMvc) {

    @TestConfiguration
    class FakeRepoConfig {
        @Bean
        fun tradeRepository(): TradeRepository = object : TradeRepository {
            override fun save(trade: Trade) = true
            override fun saveAll(trades: List<Trade>) = trades.size
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
