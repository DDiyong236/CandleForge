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
