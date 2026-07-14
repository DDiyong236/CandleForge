package com.candleforge.storage

import com.candleforge.domain.Trade
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@SpringBootTest
@Transactional
class JdbcTradeRepositoryTest @Autowired constructor(
    private val repository: TradeRepository,
) {
    private fun sample(seqId: Long, price: String = "50000000") = Trade(
        time = Instant.ofEpochMilli(1700000000000),
        code = "KRW-BTC",
        price = BigDecimal(price),
        volume = BigDecimal("0.001"),
        askBid = "BID",
        sequentialId = seqId,
    )

    @Test
    fun `새 체결을 저장하면 true를 반환하고 조회된다`() {
        val saved = repository.save(sample(seqId = 1L))
        assertTrue(saved)

        val recent = repository.findRecent("KRW-BTC", 10)
        assertEquals(1, recent.size)
        assertEquals(1L, recent.first().sequentialId)
        assertEquals(0, recent.first().price.compareTo(BigDecimal("50000000")))
    }

    @Test
    fun `같은 (code,time,sequential_id) 중복 저장은 false를 반환한다`() {
        assertTrue(repository.save(sample(seqId = 2L)))
        assertFalse(repository.save(sample(seqId = 2L)))  // 멱등: 두 번째는 무시
    }

    @Test
    fun `findRecent는 시간 내림차순으로 반환한다`() {
        val older = sample(seqId = 10L).copy(time = Instant.ofEpochMilli(1000))
        val newer = sample(seqId = 11L).copy(time = Instant.ofEpochMilli(2000))
        repository.save(older)
        repository.save(newer)

        val recent = repository.findRecent("KRW-BTC", 10)
        assertEquals(11L, recent.first().sequentialId)  // 최신이 먼저
    }
}
