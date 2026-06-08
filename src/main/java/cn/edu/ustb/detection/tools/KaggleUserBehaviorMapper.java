package cn.edu.ustb.detection.tools;

import cn.edu.ustb.detection.model.UserBehavior;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Convert generic Kaggle CSV rows to {@link UserBehavior}.
 */
public class KaggleUserBehaviorMapper {

    private static final long IEEE_REFERENCE_EPOCH_MS = 1512086400000L;
    private static final DateTimeFormatter[] TIME_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")};

    public enum Profile {
        AUTO, MULTI_CATEGORY, CLICKSTREAM, IEEE_CIS
    }

    public UserBehavior map(Map<String, String> row, Profile profile) {
        if (row == null || row.isEmpty()) {
            return null;
        }
        Profile detected = profile == Profile.AUTO ? detectProfile(row) : profile;
        switch (detected) {
            case MULTI_CATEGORY :
                return mapMultiCategory(row);
            case CLICKSTREAM :
                return mapClickstream(row);
            case IEEE_CIS :
                return mapIeee(row);
            case AUTO :
            default :
                return mapGeneric(row);
        }
    }

    private Profile detectProfile(Map<String, String> row) {
        if (hasAny(row, "TransactionDT", "TransactionID", "isFraud")) {
            return Profile.IEEE_CIS;
        }
        if (hasAny(row, "event_time", "event_type", "user_session")) {
            return Profile.MULTI_CATEGORY;
        }
        if (hasAny(row, "session ID", "page 1 (main category)", "page 2 (clothing model)")) {
            return Profile.CLICKSTREAM;
        }
        return Profile.AUTO;
    }

    private UserBehavior mapMultiCategory(Map<String, String> row) {
        String userId = firstNotBlank(row, "user_id", "userId").orElse(null);
        String action = firstNotBlank(row, "event_type", "action_type", "actionType").orElse("VIEW");
        String sessionId = firstNotBlank(row, "user_session", "session_id", "sessionId").orElse(null);
        String productId = firstNotBlank(row, "product_id", "productId").orElse(null);
        String ip = firstNotBlank(row, "ip", "ip_address").orElse("unknown-ip");
        long timestamp = parseTime(firstNotBlank(row, "event_time", "timestamp").orElse(null));
        Double amount = parseDouble(firstNotBlank(row, "price", "amount", "TransactionAmt").orElse(null));

        return UserBehavior.builder().userId(nonEmptyOrDefault(userId, sessionId, "unknown-user")).actionType(action)
                .ip(ip).timestamp(timestamp).sessionId(sessionId).productId(productId).amount(amount).build();
    }

    private UserBehavior mapClickstream(Map<String, String> row) {
        String sessionId = firstNotBlank(row, "session ID", "session_id", "sessionId", "user_session").orElse(null);
        String userId = firstNotBlank(row, "user_id", "userId").orElse(sessionId);
        String action = firstNotBlank(row, "event_type", "action_type").orElse("VIEW");
        String productId = firstNotBlank(row, "page 2 (clothing model)", "product_id").orElse(null);
        String ip = firstNotBlank(row, "country", "ip").orElse("unknown-ip");

        long timestamp = parseClickstreamDate(row);
        return UserBehavior.builder().userId(nonEmptyOrDefault(userId, sessionId, "unknown-user")).actionType(action)
                .ip(ip).timestamp(timestamp).sessionId(sessionId).productId(productId).build();
    }

    private UserBehavior mapIeee(Map<String, String> row) {
        long transactionDt = parseLong(firstNotBlank(row, "TransactionDT").orElse(null));
        long timestamp = transactionDt > 0
                ? IEEE_REFERENCE_EPOCH_MS + (transactionDt * 1000L)
                : System.currentTimeMillis();

        String userId = firstNotBlank(row, "card1", "user_id", "TransactionID").orElse("unknown-user");
        String ip = buildIeeeAddress(row);
        Double amount = parseDouble(firstNotBlank(row, "TransactionAmt", "amount").orElse(null));
        String sessionId = firstNotBlank(row, "TransactionID", "session_id").orElse(null);
        String action = parseLong(firstNotBlank(row, "isFraud").orElse("0")) == 1L ? "PAYMENT_FRAUD" : "PAYMENT";

        return UserBehavior.builder().userId(userId).actionType(action).ip(ip).timestamp(timestamp).sessionId(sessionId)
                .amount(amount).build();
    }

    private UserBehavior mapGeneric(Map<String, String> row) {
        String userId = firstNotBlank(row, "userId", "user_id", "uid", "id").orElse(null);
        String action = firstNotBlank(row, "actionType", "action_type", "event_type").orElse("VIEW");
        String ip = firstNotBlank(row, "ip", "ip_address", "country").orElse("unknown-ip");
        String sessionId = firstNotBlank(row, "sessionId", "session_id", "user_session").orElse(null);
        String productId = firstNotBlank(row, "productId", "product_id").orElse(null);
        Double amount = parseDouble(firstNotBlank(row, "amount", "price", "TransactionAmt").orElse(null));
        long timestamp = parseTime(firstNotBlank(row, "timestamp", "event_time", "TransactionDT").orElse(null));
        if (timestamp <= 0) {
            timestamp = System.currentTimeMillis();
        }

        return UserBehavior.builder().userId(nonEmptyOrDefault(userId, sessionId, "unknown-user")).actionType(action)
                .ip(ip).timestamp(timestamp).sessionId(sessionId).productId(productId).amount(amount).build();
    }

    private static Optional<String> firstNotBlank(Map<String, String> row, String... keys) {
        return Arrays.stream(keys).map(k -> getCaseInsensitive(row, k)).filter(v -> v != null && !v.trim().isEmpty())
                .map(String::trim).findFirst();
    }

    private static String getCaseInsensitive(Map<String, String> row, String key) {
        for (Map.Entry<String, String> e : row.entrySet()) {
            if (e.getKey() != null && e.getKey().equalsIgnoreCase(key)) {
                return e.getValue();
            }
        }
        return null;
    }

    private static boolean hasAny(Map<String, String> row, String... keys) {
        return Arrays.stream(keys).anyMatch(k -> getCaseInsensitive(row, k) != null);
    }

    private static long parseClickstreamDate(Map<String, String> row) {
        long year = parseLong(firstNotBlank(row, "year").orElse("1970"));
        long month = parseLong(firstNotBlank(row, "month").orElse("1"));
        long day = parseLong(firstNotBlank(row, "day").orElse("1"));
        try {
            return LocalDateTime.of((int) year, (int) month, (int) day, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private static String buildIeeeAddress(Map<String, String> row) {
        String addr1 = firstNotBlank(row, "addr1").orElse("na");
        String addr2 = firstNotBlank(row, "addr2").orElse("na");
        return "addr-" + addr1 + "-" + addr2;
    }

    private static long parseTime(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return -1L;
        }
        String value = raw.trim();
        if (value.endsWith(" UTC")) {
            value = value.substring(0, value.length() - 4).trim();
        }
        long numeric = parseLong(value);
        if (numeric > 0) {
            if (numeric < 10_000_000_000L) {
                return numeric * 1000L;
            }
            if (numeric < 200_000_000L) {
                return IEEE_REFERENCE_EPOCH_MS + numeric * 1000L;
            }
            return numeric;
        }
        try {
            return Instant.parse(value).toEpochMilli();
        } catch (DateTimeParseException ignore) {
        }
        for (DateTimeFormatter format : TIME_FORMATS) {
            try {
                return LocalDateTime.parse(value, format).toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (DateTimeParseException ignore) {
            }
        }
        return -1L;
    }

    private static long parseLong(String raw) {
        if (raw == null) {
            return -1L;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (Exception e) {
            return -1L;
        }
    }

    private static Double parseDouble(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static String nonEmptyOrDefault(String primary, String secondary, String fallback) {
        if (primary != null && !primary.trim().isEmpty()) {
            return primary.trim();
        }
        if (secondary != null && !secondary.trim().isEmpty()) {
            return secondary.trim();
        }
        return fallback.toLowerCase(Locale.ROOT);
    }
}
