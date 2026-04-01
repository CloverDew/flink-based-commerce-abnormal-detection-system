package cn.edu.ustb.detection.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** 数据模型类单元测试 */
public class ModelTest {

    @Nested
    @DisplayName("UserBehavior Tests")
    class UserBehaviorTest {

        @Test
        @DisplayName("Valid event should pass validation")
        void testValidEvent() {
            UserBehavior event = UserBehavior.builder().userId("user123").actionType("LOGIN").ip("192.168.1.1")
                    .timestamp(System.currentTimeMillis()).build();

            assertTrue(event.isValid());
        }

        @Test
        @DisplayName("Event without userId should fail validation")
        void testEventWithoutUserId() {
            UserBehavior event = UserBehavior.builder().actionType("LOGIN").ip("192.168.1.1")
                    .timestamp(System.currentTimeMillis()).build();

            assertFalse(event.isValid());
        }

        @Test
        @DisplayName("Event without actionType should fail validation")
        void testEventWithoutActionType() {
            UserBehavior event = UserBehavior.builder().userId("user123").ip("192.168.1.1")
                    .timestamp(System.currentTimeMillis()).build();

            assertFalse(event.isValid());
        }

        @Test
        @DisplayName("Event with zero timestamp should fail validation")
        void testEventWithZeroTimestamp() {
            UserBehavior event = UserBehavior.builder().userId("user123").actionType("LOGIN").ip("192.168.1.1")
                    .timestamp(0).build();

            assertFalse(event.isValid());
        }

        @Test
        @DisplayName("Builder should create complete event")
        void testBuilder() {
            long now = System.currentTimeMillis();
            UserBehavior event = UserBehavior.builder().userId("user123").actionType("PAYMENT").ip("192.168.1.1")
                    .timestamp(now).sessionId("session-abc").deviceId("device-xyz").productId("prod-001").amount(99.99)
                    .extra("{\"source\":\"app\"}").build();

            assertEquals("user123", event.getUserId());
            assertEquals("PAYMENT", event.getActionType());
            assertEquals("192.168.1.1", event.getIp());
            assertEquals(now, event.getTimestamp());
            assertEquals("session-abc", event.getSessionId());
            assertEquals("device-xyz", event.getDeviceId());
            assertEquals("prod-001", event.getProductId());
            assertEquals(99.99, event.getAmount());
            assertEquals("{\"source\":\"app\"}", event.getExtra());
        }

        @Test
        @DisplayName("Equals and hashCode should work correctly")
        void testEqualsAndHashCode() {
            long now = System.currentTimeMillis();
            UserBehavior event1 = UserBehavior.builder().userId("user123").actionType("LOGIN").ip("192.168.1.1")
                    .timestamp(now).sessionId("session-abc").build();

            UserBehavior event2 = UserBehavior.builder().userId("user123").actionType("LOGIN").ip("192.168.1.1")
                    .timestamp(now).sessionId("session-abc").build();

            assertEquals(event1, event2);
            assertEquals(event1.hashCode(), event2.hashCode());
        }
    }

    @Nested
    @DisplayName("RiskRule Tests")
    class RiskRuleTest {

        @Test
        @DisplayName("Valid rule should pass validation")
        void testValidRule() {
            RiskRule rule = createValidRule();
            assertTrue(rule.isValid());
        }

        @Test
        @DisplayName("Rule without ruleId should fail validation")
        void testRuleWithoutRuleId() {
            RiskRule rule = RiskRule.builder().ruleName("Test Rule").ruleType(RiskRule.RuleType.CREDENTIAL_STUFFING)
                    .targetActionType("LOGIN_FAIL").windowSizeMs(60000).threshold(3).build();

            assertFalse(rule.isValid());
        }

        @Test
        @DisplayName("Rule without ruleType should fail validation")
        void testRuleWithoutRuleType() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Test Rule").targetActionType("LOGIN_FAIL")
                    .windowSizeMs(60000).threshold(3).build();

            assertFalse(rule.isValid());
        }

        @Test
        @DisplayName("Rule with zero windowSize should fail validation")
        void testRuleWithZeroWindowSize() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Test Rule")
                    .ruleType(RiskRule.RuleType.CREDENTIAL_STUFFING).targetActionType("LOGIN_FAIL").windowSizeMs(0)
                    .threshold(3).build();

            assertFalse(rule.isValid());
        }

        @Test
        @DisplayName("Rule with zero threshold should fail validation")
        void testRuleWithZeroThreshold() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Test Rule")
                    .ruleType(RiskRule.RuleType.CREDENTIAL_STUFFING).targetActionType("LOGIN_FAIL").windowSizeMs(60000)
                    .threshold(0).build();

            assertFalse(rule.isValid());
        }

        @Test
        @DisplayName("Enabled rule should return isEnabled true")
        void testIsEnabled() {
            RiskRule rule = createValidRule();
            rule.setStatus(RiskRule.RuleStatus.ENABLED);
            assertTrue(rule.isEnabled());
        }

        @Test
        @DisplayName("Disabled rule should return isEnabled false")
        void testIsDisabled() {
            RiskRule rule = createValidRule();
            rule.setStatus(RiskRule.RuleStatus.DISABLED);
            assertFalse(rule.isEnabled());
        }

        @Test
        @DisplayName("Default status should be ENABLED")
        void testDefaultStatus() {
            RiskRule rule = new RiskRule();
            assertEquals(RiskRule.RuleStatus.ENABLED, rule.getStatus());
        }

        @Test
        @DisplayName("Default groupKeyType should be BY_USER_ID")
        void testDefaultGroupKeyType() {
            RiskRule rule = new RiskRule();
            assertEquals(RiskRule.GroupKeyType.BY_USER_ID, rule.getGroupKeyType());
        }

        private RiskRule createValidRule() {
            return RiskRule.builder().ruleId("rule-001").ruleName("Test Rule")
                    .ruleType(RiskRule.RuleType.CREDENTIAL_STUFFING).targetActionType("LOGIN_FAIL").windowSizeMs(60000)
                    .threshold(3).groupKeyType(RiskRule.GroupKeyType.BY_IP).version(1).build();
        }
    }

    @Nested
    @DisplayName("AlertEvent Tests")
    class AlertEventTest {

        @Test
        @DisplayName("fromRuleMatch should create valid alert")
        void testFromRuleMatch() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Credential Stuffing")
                    .ruleType(RiskRule.RuleType.CREDENTIAL_STUFFING).targetActionType("LOGIN_FAIL").windowSizeMs(60000)
                    .threshold(3).build();

            long baseTime = System.currentTimeMillis();
            List<UserBehavior> events = Arrays.asList(createEvent("user1", "192.168.1.1", baseTime),
                    createEvent("user2", "192.168.1.1", baseTime + 10000),
                    createEvent("user3", "192.168.1.1", baseTime + 20000));

            AlertEvent alert = AlertEvent.fromRuleMatch(rule, events);

            assertNotNull(alert.getAlertId());
            assertEquals("rule-001", alert.getRuleId());
            assertEquals(RiskRule.RuleType.CREDENTIAL_STUFFING, alert.getRuleType());
            assertEquals(3, alert.getMatchCount());
            assertEquals("user1", alert.getUserId());
            assertEquals("192.168.1.1", alert.getIp());
            assertEquals(baseTime, alert.getFirstEventTimestamp());
            assertEquals(baseTime + 20000, alert.getLastEventTimestamp());
            assertNotNull(alert.getMessage());
        }

        @Test
        @DisplayName("Alert level should be MEDIUM for threshold match")
        void testAlertLevelMedium() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Test")
                    .ruleType(RiskRule.RuleType.HIGH_FREQ_ACCESS).targetActionType("VIEW").windowSizeMs(60000)
                    .threshold(3).build();

            List<UserBehavior> events = Arrays.asList(createEvent("user1", "192.168.1.1", 1000),
                    createEvent("user1", "192.168.1.1", 2000), createEvent("user1", "192.168.1.1", 3000));

            AlertEvent alert = AlertEvent.fromRuleMatch(rule, events);
            assertEquals(AlertEvent.AlertLevel.MEDIUM, alert.getLevel());
        }

        @Test
        @DisplayName("Alert level should be HIGH for 2x threshold")
        void testAlertLevelHigh() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Test")
                    .ruleType(RiskRule.RuleType.HIGH_FREQ_ACCESS).targetActionType("VIEW").windowSizeMs(60000)
                    .threshold(3).build();

            List<UserBehavior> events = Arrays.asList(createEvent("user1", "192.168.1.1", 1000),
                    createEvent("user1", "192.168.1.1", 2000), createEvent("user1", "192.168.1.1", 3000),
                    createEvent("user1", "192.168.1.1", 4000), createEvent("user1", "192.168.1.1", 5000),
                    createEvent("user1", "192.168.1.1", 6000));

            AlertEvent alert = AlertEvent.fromRuleMatch(rule, events);
            assertEquals(AlertEvent.AlertLevel.HIGH, alert.getLevel());
        }

        @Test
        @DisplayName("Alert level should be CRITICAL for 3x threshold")
        void testAlertLevelCritical() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Test")
                    .ruleType(RiskRule.RuleType.HIGH_FREQ_ACCESS).targetActionType("VIEW").windowSizeMs(60000)
                    .threshold(3).build();

            List<UserBehavior> events = Arrays.asList(createEvent("user1", "192.168.1.1", 1000),
                    createEvent("user1", "192.168.1.1", 2000), createEvent("user1", "192.168.1.1", 3000),
                    createEvent("user1", "192.168.1.1", 4000), createEvent("user1", "192.168.1.1", 5000),
                    createEvent("user1", "192.168.1.1", 6000), createEvent("user1", "192.168.1.1", 7000),
                    createEvent("user1", "192.168.1.1", 8000), createEvent("user1", "192.168.1.1", 9000));

            AlertEvent alert = AlertEvent.fromRuleMatch(rule, events);
            assertEquals(AlertEvent.AlertLevel.CRITICAL, alert.getLevel());
        }

        @Test
        @DisplayName("fromRuleMatch with null rule should throw exception")
        void testFromRuleMatchWithNullRule() {
            List<UserBehavior> events = Arrays.asList(createEvent("user1", "192.168.1.1", 1000));

            assertThrows(IllegalArgumentException.class, () -> AlertEvent.fromRuleMatch(null, events));
        }

        @Test
        @DisplayName("fromRuleMatch with empty events should throw exception")
        void testFromRuleMatchWithEmptyEvents() {
            RiskRule rule = RiskRule.builder().ruleId("rule-001").ruleName("Test")
                    .ruleType(RiskRule.RuleType.HIGH_FREQ_ACCESS).targetActionType("VIEW").windowSizeMs(60000)
                    .threshold(3).build();

            assertThrows(IllegalArgumentException.class, () -> AlertEvent.fromRuleMatch(rule, Arrays.asList()));
        }

        private UserBehavior createEvent(String userId, String ip, long timestamp) {
            return UserBehavior.builder().userId(userId).actionType("LOGIN_FAIL").ip(ip).timestamp(timestamp).build();
        }
    }
}
