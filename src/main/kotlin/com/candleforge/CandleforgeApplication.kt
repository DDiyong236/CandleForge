package com.candleforge

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CandleforgeApplication

fun main(args: Array<String>) {
	runApplication<CandleforgeApplication>(*args)
}
