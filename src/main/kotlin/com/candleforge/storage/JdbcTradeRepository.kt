package com.candleforge.storage

import com.candleforge.domain.Trade
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.sql.PreparedStatement
import java.time.OffsetDateTime
import java.time.ZoneOffset

@Repository
class JdbcTradeRepository(
    private val jdbcClient: JdbcClient,
    private val jdbcTemplate: JdbcTemplate,
) : TradeRepository {

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

    override fun saveAll(trades: List<Trade>): Int {
        if (trades.isEmpty()) return 0
        val sql = """
            INSERT INTO trades (time, code, price, volume, ask_bid, sequential_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (code, "time", sequential_id) DO NOTHING
        """.trimIndent()
        val results = jdbcTemplate.batchUpdate(sql, object : BatchPreparedStatementSetter {
            override fun setValues(ps: PreparedStatement, i: Int) {
                val t = trades[i]
                ps.setObject(1, t.time.atOffset(ZoneOffset.UTC))
                ps.setString(2, t.code)
                ps.setBigDecimal(3, t.price)
                ps.setBigDecimal(4, t.volume)
                ps.setString(5, t.askBid)
                ps.setLong(6, t.sequentialId)
            }

            override fun getBatchSize(): Int = trades.size
        })
        return results.count { it > 0 }   // 신규 삽입된 행만(중복은 0)
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
