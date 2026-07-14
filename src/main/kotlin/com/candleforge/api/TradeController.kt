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
