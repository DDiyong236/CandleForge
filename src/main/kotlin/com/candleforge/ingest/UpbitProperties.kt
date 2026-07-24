package com.candleforge.ingest

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "candleforge.upbit")
data class UpbitProperties(
    val enabled: Boolean = true,
    val endpoint: String = "wss://api.upbit.com/websocket/v1",
    val codes: List<String> = listOf("KRW-BTC"),
    val dynamicCodes: Boolean = true,  // true면 전체 KRW 마켓을 REST로 조회해 구독
)
