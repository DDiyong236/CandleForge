package com.candleforge.storage

import com.candleforge.domain.Trade
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime
import java.time.ZoneOffset

@Repository
class JdbcTradeRepository(private val jdbcClient: JdbcClient) : TradeRepository {

    override fun save(trade: Trade): Boolean {
        val updated = jdbcClient.sql(
            """
            INSERT INTO trades (time, code, price, volume, ask_bid, sequential_id)
            VALUES (:time, :code, :price, :volume, :askBid, :sequentialId)
            ON CONFLICT (code, "time", sequential_id) DO NOTHING
            """.trimIndent(),
        )
            .param("time", trade.time.atOffset(ZoneOffset.UTC))
            .param("code", trade.code)
            .param("price", trade.price)
            .param("volume", trade.volume)
            .param("askBid", trade.askBid)
            .param("sequentialId", trade.sequentialId)
            .update()
        return updated == 1
    }

    override fun findRecent(code: String, limit: Int): List<Trade> =
        jdbcClient.sql(
            """
            SELECT time, code, price, volume, ask_bid, sequential_id
            FROM trades
            WHERE code = :code
            ORDER BY time DESC
            LIMIT :limit
            """.trimIndent(),
        )
            .param("code", code)
            .param("limit", limit)
            .query { rs, _ ->
                Trade(
                    time = rs.getObject("time", OffsetDateTime::class.java).toInstant(),
                    code = rs.getString("code"),
                    price = rs.getBigDecimal("price"),
                    volume = rs.getBigDecimal("volume"),
                    askBid = rs.getString("ask_bid"),
                    sequentialId = rs.getLong("sequential_id"),
                )
            }
            .list()
}
