package cn.edu.ustb.detection.serialization;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 序列化/反序列化测试 */
public class SerializationTest {

    private JsonDeserializationSchema<UserBehavior> behaviorDeserializer;
    private JsonDeserializationSchema<RiskRule> ruleDeserializer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() throws Exception {
        behaviorDeserializer = new JsonDeserializationSchema<>(UserBehavior.class);
        ruleDeserializer = new JsonDeserializationSchema<>(RiskRule.class);
        objectMapper = new ObjectMapper();
    }

    @Test
    @DisplayName("Should deserialize valid UserBehavior JSON")
    void testDeserializeUserBehavior() throws Exception {
        String json = "{" + "\"userId\":\"user123\"," + "\"actionType\":\"LOGIN\"," + "\"ip\":\"192.168.1.1\","
                + "\"timestamp\":1700000000000," + "\"sessionId\":\"session-abc\"," + "\"deviceId\":\"device-xyz\""
                + "}";

        UserBehavior event = behaviorDeserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertNotNull(event);
        assertEquals("user123", event.getUserId());
        assertEquals("LOGIN", event.getActionType());
        assertEquals("192.168.1.1", event.getIp());
        assertEquals(1700000000000L, event.getTimestamp());
        assertEquals("session-abc", event.getSessionId());
        assertEquals("device-xyz", event.getDeviceId());
    }

    @Test
    @DisplayName("Should deserialize valid RiskRule JSON")
    void testDeserializeRiskRule() throws Exception {
        String json = "{" + "\"ruleId\":\"rule-001\"," + "\"ruleName\":\"Credential Stuffing Detection\","
                + "\"ruleType\":\"CREDENTIAL_STUFFING\"," + "\"status\":\"ENABLED\","
                + "\"targetActionType\":\"LOGIN_FAIL\"," + "\"windowSizeMs\":60000," + "\"threshold\":3,"
                + "\"groupKeyType\":\"BY_IP\"," + "\"priority\":10," + "\"version\":1" + "}";

        RiskRule rule = ruleDeserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertNotNull(rule);
        assertEquals("rule-001", rule.getRuleId());
        assertEquals("Credential Stuffing Detection", rule.getRuleName());
        assertEquals(RiskRule.RuleType.CREDENTIAL_STUFFING, rule.getRuleType());
        assertEquals(RiskRule.RuleStatus.ENABLED, rule.getStatus());
        assertEquals("LOGIN_FAIL", rule.getTargetActionType());
        assertEquals(60000, rule.getWindowSizeMs());
        assertEquals(3, rule.getThreshold());
        assertEquals(RiskRule.GroupKeyType.BY_IP, rule.getGroupKeyType());
        assertEquals(10, rule.getPriority());
        assertEquals(1, rule.getVersion());
    }

    @Test
    @DisplayName("Should return null for null input")
    void testDeserializeNull() throws Exception {
        UserBehavior event = behaviorDeserializer.deserialize(null);
        assertNull(event);
    }

    @Test
    @DisplayName("Should return null for empty input")
    void testDeserializeEmpty() throws Exception {
        UserBehavior event = behaviorDeserializer.deserialize(new byte[0]);
        assertNull(event);
    }

    @Test
    @DisplayName("Should return null for invalid JSON")
    void testDeserializeInvalidJson() throws Exception {
        String invalidJson = "not a valid json";
        UserBehavior event = behaviorDeserializer.deserialize(invalidJson.getBytes(StandardCharsets.UTF_8));
        assertNull(event);
    }

    @Test
    @DisplayName("Should ignore unknown properties")
    void testIgnoreUnknownProperties() throws Exception {
        String json = "{" + "\"userId\":\"user123\"," + "\"actionType\":\"LOGIN\"," + "\"ip\":\"192.168.1.1\","
                + "\"timestamp\":1700000000000," + "\"unknownField\":\"should be ignored\"" + "}";

        UserBehavior event = behaviorDeserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertNotNull(event);
        assertEquals("user123", event.getUserId());
    }

    @Test
    @DisplayName("Should handle unknown enum values gracefully")
    void testUnknownEnumValue() throws Exception {
        String json = "{" + "\"ruleId\":\"rule-001\"," + "\"ruleName\":\"Test Rule\","
                + "\"ruleType\":\"UNKNOWN_TYPE\"," + "\"targetActionType\":\"LOGIN_FAIL\"," + "\"windowSizeMs\":60000,"
                + "\"threshold\":3" + "}";

        RiskRule rule = ruleDeserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertNotNull(rule);
        assertEquals("rule-001", rule.getRuleId());
        assertNull(rule.getRuleType());
    }

    @Test
    @DisplayName("Should handle partial JSON (missing optional fields)")
    void testPartialJson() throws Exception {
        String json = "{" + "\"userId\":\"user123\"," + "\"actionType\":\"LOGIN\"," + "\"timestamp\":1700000000000"
                + "}";

        UserBehavior event = behaviorDeserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertNotNull(event);
        assertEquals("user123", event.getUserId());
        assertEquals("LOGIN", event.getActionType());
        assertNull(event.getIp());
        assertNull(event.getSessionId());
    }

    @Test
    @DisplayName("Should return correct TypeInformation")
    void testProducedType() {
        assertEquals(UserBehavior.class, behaviorDeserializer.getProducedType().getTypeClass());
        assertEquals(RiskRule.class, ruleDeserializer.getProducedType().getTypeClass());
    }

    @Test
    @DisplayName("isEndOfStream should always return false")
    void testIsEndOfStream() {
        assertFalse(behaviorDeserializer.isEndOfStream(null));
        assertFalse(behaviorDeserializer.isEndOfStream(new UserBehavior()));
    }
}
