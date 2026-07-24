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

        client.connect(listOf("KRW-BTC")) { json ->
            println("수신: ${json.take(120)}")
            if (json.contains("\"type\":\"trade\"") || json.contains("\"trade\"")) latch.countDown()
        }

        val got = latch.await(15, TimeUnit.SECONDS)
        client.close()
        assertTrue(got, "15초 내 체결을 받지 못함")
    }
}
