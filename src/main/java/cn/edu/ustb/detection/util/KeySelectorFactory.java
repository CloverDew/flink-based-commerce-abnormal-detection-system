package cn.edu.ustb.detection.util;

import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 键选择器工厂
 *
 * <p>
 * 根据规则的分组键类型生成对应的 KeySelector。 支持按用户ID、IP地址、设备ID等维度进行分组。
 */
public class KeySelectorFactory {

    private KeySelectorFactory() {
    }

    /** 为事件-规则对创建键选择器 根据规则中指定的分组键类型提取对应的 key */
    public static KeySelector<Tuple2<UserBehavior, RiskRule>, String> createKeySelector() {
        return new KeySelector<Tuple2<UserBehavior, RiskRule>, String>() {
            @Override
            public String getKey(Tuple2<UserBehavior, RiskRule> value) throws Exception {
                UserBehavior event = value.f0;
                RiskRule rule = value.f1;

                if (event == null || rule == null) {
                    return "UNKNOWN";
                }

                RiskRule.GroupKeyType keyType = rule.getGroupKeyType();
                if (keyType == null) {
                    keyType = RiskRule.GroupKeyType.BY_USER_ID;
                }

                String keyValue;
                switch (keyType) {
                    case BY_IP :
                        keyValue = event.getIp();
                        break;
                    case BY_DEVICE_ID :
                        keyValue = event.getDeviceId();
                        break;
                    case BY_SESSION_ID :
                        keyValue = event.getSessionId();
                        break;
                    case BY_USER_ID :
                    default :
                        keyValue = event.getUserId();
                        break;
                }

                if (keyValue == null || keyValue.isEmpty()) {
                    keyValue = "UNKNOWN";
                }

                return rule.getRuleId() + ":" + keyValue;
            }
        };
    }

    /** 为用户行为事件创建按用户ID分组的键选择器 */
    public static KeySelector<UserBehavior, String> byUserId() {
        return event -> event != null && event.getUserId() != null ? event.getUserId() : "UNKNOWN";
    }

    /** 为用户行为事件创建按IP分组的键选择器 */
    public static KeySelector<UserBehavior, String> byIp() {
        return event -> event != null && event.getIp() != null ? event.getIp() : "UNKNOWN";
    }

    /** 为用户行为事件创建按设备ID分组的键选择器 */
    public static KeySelector<UserBehavior, String> byDeviceId() {
        return event -> event != null && event.getDeviceId() != null ? event.getDeviceId() : "UNKNOWN";
    }
}
