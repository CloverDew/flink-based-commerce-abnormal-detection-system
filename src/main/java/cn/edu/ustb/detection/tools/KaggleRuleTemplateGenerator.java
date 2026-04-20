package cn.edu.ustb.detection.tools;

import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.RiskRule.GroupKeyType;
import cn.edu.ustb.detection.model.RiskRule.RuleStatus;
import cn.edu.ustb.detection.model.RiskRule.RuleType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate rule templates for Kaggle profiles.
 */
public class KaggleRuleTemplateGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(KaggleRuleTemplateGenerator.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String profile = params.getOrDefault("profile", "multi_category").trim().toLowerCase();
        String output = params.getOrDefault("output", "samples/generated-risk-rules.json");
        int version = parseInt(params.getOrDefault("version", "1"), 1);

        List<RiskRule> rules = buildRules(profile, version, System.currentTimeMillis());
        if (rules.isEmpty()) {
            throw new IllegalArgumentException(
                    "Unsupported profile: " + profile + ", available: multi_category, clickstream, ieee_cis");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        if (Paths.get(output).getParent() != null) {
            Files.createDirectories(Paths.get(output).getParent());
        }

        try (Writer writer = Files.newBufferedWriter(Paths.get(output))) {
            mapper.writeValue(writer, rules);
        }

        LOG.info("Generated {} rules for profile={} -> {}", rules.size(), profile, output);
    }

    public static List<RiskRule> buildRules(String profile, int version, long now) {
        switch (profile) {
            case "multi_category" :
            case "multi-category" :
            case "mkechinov" :
                return buildMultiCategoryRules(version, now);
            case "clickstream" :
            case "tunguz" :
                return buildClickstreamRules(version, now);
            case "ieee_cis" :
            case "ieee-cis" :
            case "ieee" :
                return buildIeeeRules(version, now);
            default :
                return new ArrayList<>();
        }
    }

    private static List<RiskRule> buildMultiCategoryRules(int version, long now) {
        List<RiskRule> rules = new ArrayList<>();
        rules.add(rule("mc-login-fail-burst", "多分类电商-登录失败突增", RuleType.CREDENTIAL_STUFFING, "LOGIN_FAIL", 60000L, 3,
                GroupKeyType.BY_IP, 10, version, now, "同一IP在1分钟内出现>=3次登录失败"));
        rules.add(rule("mc-order-brush-user", "多分类电商-用户高频下单", RuleType.ORDER_BRUSH, "PURCHASE", 120000L, 5,
                GroupKeyType.BY_USER_ID, 20, version, now, "同一用户2分钟内出现>=5次购买行为"));
        rules.add(rule("mc-cart-bot-session", "多分类电商-会话高频加购", RuleType.HIGH_FREQ_ACCESS, "ADD_CART", 30000L, 20,
                GroupKeyType.BY_SESSION_ID, 30, version, now, "同一会话30秒内>=20次加购，疑似脚本"));
        return rules;
    }

    private static List<RiskRule> buildClickstreamRules(int version, long now) {
        List<RiskRule> rules = new ArrayList<>();
        rules.add(rule("cs-page-view-burst", "Clickstream-高频浏览", RuleType.HIGH_FREQ_ACCESS, "VIEW", 20000L, 50,
                GroupKeyType.BY_SESSION_ID, 10, version, now, "同一会话20秒内>=50次浏览"));
        rules.add(rule("cs-order-brush", "Clickstream-疑似刷单", RuleType.ORDER_BRUSH, "ORDER", 180000L, 6,
                GroupKeyType.BY_USER_ID, 20, version, now, "同一用户3分钟>=6次下单"));
        rules.add(rule("cs-cart-burst", "Clickstream-高频加购", RuleType.HIGH_FREQ_ACCESS, "ADD_CART", 60000L, 15,
                GroupKeyType.BY_USER_ID, 30, version, now, "同一用户1分钟内>=15次加购"));
        return rules;
    }

    private static List<RiskRule> buildIeeeRules(int version, long now) {
        List<RiskRule> rules = new ArrayList<>();
        rules.add(rule("ieee-payment-fraud", "IEEE-CIS-支付欺诈高频", RuleType.PAYMENT_FRAUD, "PAYMENT_FRAUD", 300000L, 3,
                GroupKeyType.BY_USER_ID, 5, version, now, "同一账户5分钟内>=3笔疑似欺诈交易"));
        rules.add(rule("ieee-payment-burst", "IEEE-CIS-支付高频", RuleType.HIGH_FREQ_ACCESS, "PAYMENT", 120000L, 10,
                GroupKeyType.BY_USER_ID, 15, version, now, "同一账户2分钟内>=10次支付行为"));
        rules.add(rule("ieee-card-probe", "IEEE-CIS-卡号探测", RuleType.CREDENTIAL_STUFFING, "PAYMENT", 60000L, 8,
                GroupKeyType.BY_IP, 25, version, now, "同一地址1分钟内>=8次支付尝试"));
        return rules;
    }

    private static RiskRule rule(String id, String name, RuleType type, String action, long window, int threshold,
            GroupKeyType keyType, int priority, int version, long now, String desc) {
        return RiskRule.builder().ruleId(id).ruleName(name).ruleType(type).status(RuleStatus.ENABLED)
                .targetActionType(action).windowSizeMs(window).threshold(threshold).groupKeyType(keyType)
                .priority(priority).version(version).updateTimestamp(now).description(desc).build();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        if (args == null) {
            return map;
        }
        for (int i = 0; i < args.length; i++) {
            String token = args[i];
            if (!token.startsWith("--")) {
                continue;
            }
            String key = token.substring(2);
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                map.put(key, args[++i]);
            } else {
                map.put(key, "true");
            }
        }
        return map;
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
