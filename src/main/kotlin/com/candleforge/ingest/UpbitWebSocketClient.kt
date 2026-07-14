package com.candleforge.ingest

import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean

class UpbitWebSocketClient(private val properties: UpbitProperties) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val httpClient: HttpClient = HttpClient.newHttpClient()
    private val running = AtomicBoolean(false)

    @Volatile private var webSocket: WebSocket? = null

    /** 연결을 시작하고, 디코딩된 JSON 메시지마다 onMessage를 호출한다. 논블로킹. */
    fun connect(onMessage: (String) -> Unit) {
        running.set(true)
        openWithRetry(onMessage, attempt = 0)
    }

    fun close() {
        running.set(false)
        webSocket?.sendClose(WebSocket.NORMAL_CLOSURE, "bye")
    }

    private fun openWithRetry(onMessage: (String) -> Unit, attempt: Int) {
        if (!running.get()) return
        httpClient.newWebSocketBuilder()
            .buildAsync(URI.create(properties.endpoint), Listener(onMessage))
            .whenComplete { ws, err ->
                if (err != null) {
                    val backoff = minOf(30_000L, 1_000L * (1L shl minOf(attempt, 5)))
                    log.warn("Upbit 연결 실패(attempt=$attempt), ${backoff}ms 후 재시도: ${err.message}")
                    Thread.sleep(backoff)
                    openWithRetry(onMessage, attempt + 1)
                } else {
                    webSocket = ws
                    subscribe(ws)
                    log.info("Upbit 연결 성공: codes=${properties.codes}")
                }
            }
    }

    /** 구독 요청 전송: [{ticket},{type:trade,codes},{format:DEFAULT}] */
    private fun subscribe(ws: WebSocket) {
        val codesJson = properties.codes.joinToString(",") { "\"$it\"" }
        val request = """[{"ticket":"${UUID.randomUUID()}"},{"type":"trade","codes":[$codesJson]},{"format":"DEFAULT"}]"""
        ws.sendText(request, true)
        ws.request(1)
    }

    private inner class Listener(private val onMessage: (String) -> Unit) : WebSocket.Listener {
        private val buffer = StringBuilder()

        override fun onBinary(ws: WebSocket, data: ByteBuffer, last: Boolean): CompletionStage<*>? {
            buffer.append(StandardCharsets.UTF_8.decode(data))
            if (last) {
                val json = buffer.toString()
                buffer.setLength(0)
                try { onMessage(json) } catch (e: Exception) { log.debug("메시지 처리 실패: ${e.message}") }
            }
            ws.request(1)
            return null
        }

        override fun onText(ws: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            // 일부 상태 메시지는 텍스트로 올 수 있음(UP 등) — trade가 아니면 파서가 무시
            try { onMessage(data.toString()) } catch (e: Exception) { log.debug("텍스트 무시: ${e.message}") }
            ws.request(1)
            return null
        }

        override fun onError(ws: WebSocket, error: Throwable) {
            log.warn("WebSocket 에러: ${error.message}")
            reconnect()
        }

        override fun onClose(ws: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
            log.warn("WebSocket 종료: $statusCode $reason")
            reconnect()
            return null
        }

        private fun reconnect() {
            if (running.get()) {
                log.info("재연결 시도")
                openWithRetry(onMessage, attempt = 0)
            }
        }
    }

    /** idle 타임아웃(120s) 회피용 하트비트. */
    fun ping() {
        webSocket?.sendText("PING", true)
    }
}
