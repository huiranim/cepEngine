package org.example.cepengine.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Flink CEP 엔진 클래스
 * Kafka에서 수신한 실시간 이벤트 스트림을 처리하여
 * 정의된 CEP 패턴에 맞는 사용자 행동을 감지하고 후속 처리 수행
 */
@Component
public class CepEngine {

    private final StreamExecutionEnvironment env;                   // Flink 스트리밍 실행 환경 (데이터 스트림의 소스, 변환, 싱크를 관리)
    private final KafkaSource<String> kafkaSource;                  // Kafka로부터 실시간 이벤트를 읽어오는 Source
    private final WatermarkStrategy<String> watermarkStrategy;      // 워터마크 전략
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(CepEngine.class);

    public CepEngine(StreamExecutionEnvironment env,
                     KafkaSource<String> kafkaSource,
                     WatermarkStrategy<String> watermarkStrategy) {
        this.env = env;
        this.kafkaSource = kafkaSource;
        this.watermarkStrategy = watermarkStrategy;
    }

    /**
     * Flink 스트리밍 작업 시작 메서드
     * 1. Kafka → JSON 문자열 → Map 변환
     * 2. CEP 패턴 정의: 동일 유저가 특정 상품을 10분 내 3번 클릭
     * 3. CEP 패턴 스트림 생성
     * 4. 패턴 매칭 시 후속 액션 정의: 쿠폰 발급 대상 로그 출력
     * 5. Flink 스트리밍 실행
     */
    @PostConstruct
    public void runAsync() {
        // 테스트 환경에서 실행하지 않도록 조건 분기
        if (isTestEnvironment()) {
            log.info("Test environment detected, skipping Flink job execution.");
            return;
        }

        log.info("Starting Flink CEP job in a separate thread...");

        // Flink Job 실행을 별도 스레드로 분리
        // @PostConstruct에서 Flink Job을 직접 실행하게 되면, env.execute()는 무한 블로킹 호출됨 (= 메서드가 반환되지 않고 무기한 대기함)
        new Thread(() -> {
            try {
                runFlinkJob();
            } catch (Exception e) {
                log.error("Flink job execution failed", e);
            }
        }, "flink-runner-thread").start();
    }

    public void runFlinkJob() throws Exception {
        // 1. Kafka → JSON 문자열 → Map 변환
        log.info("Initializing Kafka source...");
        DataStream<Map<String, Object>> eventStream = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),  // 워터마크 비활성화
                        "KafkaSource")
                .map(new MapFunction<String, Map<String, Object>>() {
                     @Override
                     public Map<String, Object> map(String json) throws Exception {
                         try {
                             Map<String, Object> parsed = OBJECT_MAPPER.readValue(json, Map.class);
                             log.info("Received Kafka message: {}", json); // 원본 메시지
                             log.info("Parsed Kafka event: {}", parsed);   // 파싱 결과 메시지
                             
                             // product_click 이벤트인 경우 추가 로그
                             if ("product_click".equals(parsed.get("eventType"))) {
                                 log.info("*** PRODUCT_CLICK DETECTED *** userId: {}, productId: {}, timestamp: {}", 
                                         parsed.get("userId"), parsed.get("productId"), parsed.get("timestamp"));
                             }
                             
                             return parsed;
                         } catch (Exception e) {
                             log.error("Failed to parse Kafka message: {}", json, e);
                             return null;
                         }
                     }
                })
                .filter(Objects::nonNull);  // null이 아닌 것만 필터링

        log.info("Event stream created with watermark strategy applied");

        // CEP 패턴 방식 (단순한 패턴으로 테스트)
        log.info("Testing CEP with simple pattern...");
        
        // 2. CEP 패턴 정의: 단순한 product_click 이벤트 1개 감지
        log.info("Defining SIMPLE CEP pattern: detect single product_click event");
        Pattern<Map<String, Object>, ?> pattern = Pattern.<Map<String, Object>>begin("click")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Map<String, Object> event) {
                        boolean isProductClick = "product_click".equals(event.get("eventType"));
                        if (isProductClick) {
                            log.info("*** CEP PATTERN MATCH *** Event passed filter: userId={}, productId={}, timestamp={}", 
                                    event.get("userId"), event.get("productId"), event.get("timestamp"));
                        }
                        return isProductClick;
                    }
                });

        // 3. CEP 패턴 스트림 생성 - userId와 productId 조합으로 키 그룹화
        log.info("Creating CEP pattern stream with userId+productId key...");
        PatternStream<Map<String, Object>> patternStream = CEP.pattern(
                eventStream.keyBy(e -> e.get("userId") + "_" + e.get("productId")),
                pattern
        );

        // 4. 패턴 매칭 시 후속 액션 정의: 쿠폰 발급 대상 로그 출력
        log.info("Defining CEP match action...");
        patternStream.select((PatternSelectFunction<Map<String, Object>, String>) patternMatch -> {
            try {
                log.info("*** CEP PATTERN MATCHED! *** Pattern match details: {}", patternMatch);
                
                List<Map<String, Object>> clicks = patternMatch.get("click");
                log.info("*** CEP CLICKS COUNT: {} ***", clicks.size());
                
                String userId = (String) clicks.get(0).get("userId");
                String productId = (String) clicks.get(0).get("productId");
                
                String result = "[CEP 감지] userId=" + userId + ", productId=" + productId + 
                               " → 단순 패턴 매칭 성공!";
                log.info(result);
                log.info("Click details - timestamp: {}", clicks.get(0).get("timestamp"));
                return result;
            } catch (Exception e) {
                log.error("Exception occurred while processing CEP pattern match", e);
                return "[CEP 감지] Exception occurred";
            }
        }).print();

        // 단순한 filter() + count() 방식 (주석처리)
        /*
        // 단순한 filter() + count() 방식으로 테스트
        log.info("Testing simple filter() + count() approach...");
        
        // 2. product_click 이벤트만 필터링
        DataStream<Map<String, Object>> clickEvents = eventStream
                .filter(event -> {
                    boolean isProductClick = "product_click".equals(event.get("eventType"));
                    if (isProductClick) {
                        log.info("*** FILTER MATCH *** Event passed filter: userId={}, productId={}, timestamp={}", 
                                event.get("userId"), event.get("productId"), event.get("timestamp"));
                    }
                    return isProductClick;
                });

        // 3. userId+productId로 키 그룹화하고 카운트
        clickEvents.keyBy(e -> e.get("userId") + "_" + e.get("productId"))
                .countWindow(3) // 3번 클릭 시 윈도우 완료
                .apply(new org.apache.flink.streaming.api.functions.windowing.WindowFunction<Map<String, Object>, String, String, org.apache.flink.streaming.api.windowing.windows.GlobalWindow>() {
                    @Override
                    public void apply(String key, 
                                    org.apache.flink.streaming.api.windowing.windows.GlobalWindow window,
                                    Iterable<Map<String, Object>> input, 
                                    org.apache.flink.util.Collector<String> out) throws Exception {
                        try {
                            // Iterable의 크기 계산
                            int count = 0;
                            for (Map<String, Object> event : input) {
                                count++;
                            }
                            
                            log.info("*** COUNT WINDOW COMPLETED *** Key: {}, Count: {}", key, count);
                            
                            // 다시 iterator를 생성하여 데이터 처리
                            String userId = null;
                            String productId = null;
                            for (Map<String, Object> click : input) {
                                if (userId == null) {
                                    userId = (String) click.get("userId");
                                    productId = (String) click.get("productId");
                                }
                            }
                            
                            String result = "[SIMPLE 감지] userId=" + userId + ", productId=" + productId + 
                                           " → 3번 클릭 감지! 쿠폰 발급 대상!";
                            log.info(result);
                            
                            // 각 클릭의 타임스탬프 출력
                            int i = 1;
                            for (Map<String, Object> click : input) {
                                log.info("Click {}: {}", i++, click.get("timestamp"));
                            }
                            
                            out.collect(result);
                        } catch (Exception e) {
                            log.error("Exception in count window", e);
                            out.collect("[SIMPLE 감지] Exception occurred");
                        }
                    }
                }).print();
        */

        // 5. Flink 스트리밍 실행
        log.info("Executing Flink job...");
        env.execute("CEP-PILOT0-JOB");
        log.info("Flink job execution started.");
    }

    // 테스트 환경 여부
    private boolean isTestEnvironment() {
        // Spring test context에서는 "test" profile이 자동 활성화됨
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .anyMatch(ste -> ste.getClassName().startsWith("org.junit."));
    }
}