package com.candleforge.ingest

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.readValue
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@JsonIgnoreProperties(ignoreUnknown = true)
data class UpbitMarket(
    @param:JsonProperty("market") val market: String,
)

@Component
class UpbitMarketClient(private val objectMapper: ObjectMapper) {

    private val httpClient: HttpClient = HttpClient.newHttpClient()

    /** Upbit에서 거래 가능한 전체 마켓을 받아 KRW 마켓 코드만 반환. */
    fun fetchKrwMarkets(): List<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("https://api.upbit.com/v1/market/all"))
            .GET()
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        return parseKrwMarkets(response.body())
    }

    /** 마켓 목록 JSON에서 code가 "KRW-"로 시작하는 것만 추출. */
    fun parseKrwMarkets(json: String): List<String> {
        val markets: List<UpbitMarket> = objectMapper.readValue(json)
        return markets.map { it.market }.filter { it.startsWith("KRW-") }
    }
}
