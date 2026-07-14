package com.candleforge.config

import com.candleforge.ingest.UpbitProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@EnableScheduling
@EnableConfigurationProperties(UpbitProperties::class)
class AppConfig
