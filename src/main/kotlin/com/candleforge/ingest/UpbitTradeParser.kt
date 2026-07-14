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
