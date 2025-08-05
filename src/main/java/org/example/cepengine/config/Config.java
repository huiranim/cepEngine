package org.example.cepengine.config;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@Configuration
public class Config {
    /*
    * Flink 애플리케이션 실행 환경(context)
    * 데이터를 읽고(source), 처리하고(transform), 출력(sink)하는 전체 실행 플랜을 구성하는 핵심 객체
    * Flink 프로그램의 시작점이자, 마지막에 .execute()를 호출해야 실제로 실행됨
    */
    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        return StreamExecutionEnvironment.createLocalEnvironment();
    }

    /*
     * Kafka에서 실시간으로 이벤트 데이터를 읽어오기 위한 Source 정의
     * Flink가 Kafka의 특정 topic을 구독하도록 설정함
     */
    @Bean
    public KafkaSource<String> kafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers("192.168.150.115:9192,192.168.150.115:9194,192.168.150.125:9192")
                .setTopics("hr-test-cep-log")
                .setGroupId("flink-cep-test")
                .setStartingOffsets(OffsetsInitializer.latest())    // 소비 시작 위치 (latest는 가장 최근부터)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /*
     * 이벤트 시간 기반 처리를 위한 워터마크 전략 설정
     * 워터마크: Flink에서 지연된 이벤트를 다루기 위해 이벤트 스트림에 붙이는 타임마커
     *         예) 5분 늦게 도착한 이벤트도 “10분 이내 클릭”에 포함시켜야 할 경우 사용
     */
    @Bean
    public WatermarkStrategy<Map<String, Object>> watermarkStrategy() {
//        return WatermarkStrategy.noWatermarks();    // 워터마크 미설정

        return WatermarkStrategy
                .<Map<String, Object>>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 이벤트 도착 지연 허용 시간
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        String timestampStr = (String) event.get("timestamp");  // timestamp 필드를 타임스탬프로 인식하게 함
                        return Instant.parse(timestampStr).toEpochMilli(); // ISO 8601 → long
                    } catch (Exception e) {
                        return System.currentTimeMillis(); // fallback
                    }
                });
    }
}
