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

    // CEP 패턴 설정 상수
    private static final int CEP_CLICK_COUNT_THRESHOLD = 3;           // 감지할 클릭 횟수
    private static final int CEP_TIME_WINDOW_MINUTES = 10;            // 타임 윈도우 (분)
    private static final String CEP_PATTERN_NAME = "product_click_event"; // 패턴 이름

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
                        watermarkStrategy,  // CEP를 위해 워터마크 다시 활성화
                        "KafkaSource")
                .map(new MapFunction<String, Map<String, Object>>() {
                     @Override
                     public Map<String, Object> map(String json) throws Exception {
                         try {
                             Map<String, Object> parsed = OBJECT_MAPPER.readValue(json, Map.class);
                             log.info("Received Kafka message: {}", json); // 원본 메시지
                             log.info("Parsed Kafka event: {}", parsed);   // 파싱 결과 메시지
                             return parsed;
                         } catch (Exception e) {
                             log.error("Failed to parse Kafka message: {}", json, e);
                             return null;
                         }
                     }
                })
                .filter(Objects::nonNull);  // null이 아닌 것만 필터링

        log.info("Event stream created with watermark strategy applied");

        // 방법 1: 단순 필터 방식 (단순 이벤트 감지용)
//        runSimpleFilterApproach(eventStream);
        
        // 방법 2: CEP 패턴 방식 (복잡한 시간 기반 패턴용)
        runCepPatternApproach(eventStream);

        // 5. Flink 스트리밍 실행
        log.info("Executing Flink job...");
        env.execute("CEP-PILOT0-JOB");
        log.info("Flink job execution started.");
    }

    /**
     * 방법 1: 단순 필터 방식 - 단순한 이벤트 감지에 적합
     */
    private void runSimpleFilterApproach(DataStream<Map<String, Object>> eventStream) {
        log.info("=== METHOD 1: Simple Filter Approach (단순 이벤트 감지용) ===");
        
        // product_click 이벤트만 필터링
        DataStream<Map<String, Object>> clickEvents = eventStream
                .filter(event -> {
                    boolean isProductClick = "product_click".equals(event.get("eventType"));
                    if (isProductClick) {
                        log.info("*** SIMPLE FILTER MATCH *** Event passed filter: userId={}, productId={}, timestamp={}", 
                                event.get("userId"), event.get("productId"), event.get("timestamp"));
                    }
                    return isProductClick;
                });

        // 결과 출력
        clickEvents.map(event -> {
            String userId = (String) event.get("userId");
            String productId = (String) event.get("productId");
            String result = "[SIMPLE FILTER] userId=" + userId + ", productId=" + productId + " → 단순 필터 감지 성공!";
            log.info(result);
            return result;
        }).print("SimpleFilter");
    }

    /**
     * 방법 2: CEP 패턴 방식 - 복잡한 시간 기반 패턴에 적합
     * 예: 10분 내 동일 상품을 3번 클릭한 사용자 감지
     */
    private void runCepPatternApproach(DataStream<Map<String, Object>> eventStream) {
        log.info("=== METHOD 2: CEP Pattern Approach (복잡한 패턴 감지용) ===");
        
        // CEP 패턴 정의: 10분 내 동일 상품을 3번 클릭한 사용자 감지
        log.info("Defining COMPLEX CEP pattern: detect {} product_click events within {} minute for same product", 
                CEP_CLICK_COUNT_THRESHOLD, CEP_TIME_WINDOW_MINUTES);
        
        // 개별 이벤트를 감지하고 후속 처리에서 3개 확인 (카운팅 초기화를 위해)
        Pattern<Map<String, Object>, ?> pattern = Pattern.<Map<String, Object>>begin(CEP_PATTERN_NAME)
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Map<String, Object> event) {
                        log.info("*** CEP PRODUCT CLICK CONDITION *** Event: {}", event);
                        log.info("*** CEP CONDITION EXECUTION TIME *** Current time: {}", System.currentTimeMillis());
                        boolean isProductClick = "product_click".equals(event.get("eventType"));
                        if (isProductClick) {
                            log.info("*** CEP PRODUCT CLICK MATCH *** userId={}, productId={}, timestamp={}", 
                                    event.get("userId"), event.get("productId"), event.get("timestamp"));
                        } else {
                            log.info("*** CEP PRODUCT CLICK REJECTED *** Event type: {}", event.get("eventType"));
                        }
                        return isProductClick;
                    }
                })
                .times(CEP_CLICK_COUNT_THRESHOLD) // 클릭 횟수
                .within(org.apache.flink.streaming.api.windowing.time.Time.minutes(CEP_TIME_WINDOW_MINUTES)); // 타임 윈도우

        log.info("CEP pattern created successfully: {}", pattern);

        // CEP 패턴 스트림 생성 - userId+productId로 키 그룹화
        log.info("Creating CEP pattern stream with userId+productId key...");
        PatternStream<Map<String, Object>> patternStream = CEP.pattern(
                eventStream.keyBy(e -> {
                    String key = e.get("userId") + "_" + e.get("productId");
                    log.info("*** CEP KEY GROUPING *** Key: {}, Event: {}", key, e);
                    return key;
                }),
                pattern
        );

        log.info("CEP pattern stream created successfully");

        // 패턴 매칭 시 후속 액션 정의
        log.info("Defining CEP match action...");
        patternStream.select((PatternSelectFunction<Map<String, Object>, String>) patternMatch -> {
            try {
                log.info("*** CEP PATTERN MATCHED! *** Pattern match details: {}", patternMatch);
                
                List<Map<String, Object>> clicks = patternMatch.get(CEP_PATTERN_NAME);
                log.info("*** CEP CLICKS COUNT: {} ***", clicks.size());
                
                String userId = (String) clicks.get(0).get("userId");
                String productId = (String) clicks.get(0).get("productId");
                
                String result = String.format("[CEP 감지] userId=%s, productId=%s → %d분 내 %d번 클릭 감지! 쿠폰 발급 대상!", 
                        userId, productId, CEP_TIME_WINDOW_MINUTES, clicks.size());
                log.info(result);
                
                // 각 클릭의 타임스탬프 출력
                for (int i = 0; i < clicks.size(); i++) {
                    log.info("Click {}: {}", i + 1, clicks.get(i).get("timestamp"));
                }

                return result;
            } catch (Exception e) {
                log.error("Exception occurred while processing CEP pattern match", e);
                return "[CEP 감지] Exception occurred";
            }
        }).print("CepPattern");
    }

    // 테스트 환경 여부
    private boolean isTestEnvironment() {
        // Spring test context에서는 "test" profile이 자동 활성화됨
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .anyMatch(ste -> ste.getClassName().startsWith("org.junit."));
    }
}