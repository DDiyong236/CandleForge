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
