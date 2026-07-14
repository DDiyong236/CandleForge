package com.candleforge.storage

import com.candleforge.domain.Trade

interface TradeRepository {
    /** 저장 성공(신규) 시 true, 멱등 충돌(중복)로 저장 안 되면 false. */
    fun save(trade: Trade): Boolean

    /** 특정 종목의 최신 체결을 시간 내림차순으로 limit개 조회. */
    fun findRecent(code: String, limit: Int): List<Trade>
}
