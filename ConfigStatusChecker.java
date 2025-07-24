import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigStatusChecker {

    private static final Logger log = LoggerFactory.getLogger(ConfigStatusChecker.class);

    public boolean canProcessCurrentConfig(List<Stats> statsList, String aitNo, String dbType, String currentConfigId) {
        int currentConfigNumber = extractNumeric(currentConfigId);
        log.info("===> Starting validation for AIT_NO: {}, DB_TYPE: {}, CONFIG_ID: {} (Parsed: {})",
                aitNo, dbType, currentConfigId, currentConfigNumber);

        for (Stats stat : statsList) {
            if (!aitNo.equals(stat.getAitNo()) || !dbType.equals(stat.getDbType())) {
                log.debug("Skipping Stat [AIT_NO: {}, DB_TYPE: {}] - Doesn't match filter.",
                        stat.getAitNo(), stat.getDbType());
                continue;
            }

            String statConfigId = stat.getConfigId();
            int statConfigNumber = extractNumeric(statConfigId);
            log.info("Checking Config ID: {} (Parsed: {})", statConfigId, statConfigNumber);

            if (statConfigNumber <= currentConfigNumber) {
                log.debug("Ignoring Config ID {} - Not a future config (<= current).", statConfigId);
                continue;
            }

            Map<String, String> eventMap = stat.getEventMap();
            for (Map.Entry<String, String> entry : eventMap.entrySet()) {
                String eventName = entry.getKey();
                String status = entry.getValue();

                if (!isRelevantEvent(eventName)) {
                    log.debug("Skipping Event: {} - Not 'producer' or 'metadata'", eventName);
                    continue;
                }

                log.info("   ➤ Event: {} → Status: {}", eventName, status);

                if (isBlockingStatus(status)) {
                    log.warn("❌ Blocking status found in Config ID {} → Event: {}, Status: {}",
                            statConfigId, eventName, status);
                    return false;
                }
            }

            log.info("✔ No blocking status in Config ID {}", statConfigId);
        }

        log.info("✅ No blocking config found. Safe to proceed with {}", currentConfigId);
        return true;
    }

    private int extractNumeric(String configId) {
        String numberPart = configId.replaceAll("[^0-9]", "");
        int value = numberPart.isEmpty() ? 0 : Integer.parseInt(numberPart);
        log.debug("Parsed configId \"{}\" → {}", configId, value);
        return value;
    }

    private boolean isBlockingStatus(String status) {
        return "in progress".equalsIgnoreCase(status) ||
               "partially processed".equalsIgnoreCase(status);
    }

    private boolean isRelevantEvent(String event) {
        return "producer".equalsIgnoreCase(event) || "metadata".equalsIgnoreCase(event);
    }

    // Inner class for Stats
    public static class Stats {
        private String aitNo;
        private String configId;
        private String dbType;
        private Map<String, String> eventMap;

        public Stats(String aitNo, String configId, String dbType, Map<String, String> eventMap) {
            this.aitNo = aitNo;
            this.configId = configId;
            this.dbType = dbType;
            this.eventMap = eventMap;
        }

        public String getAitNo() { return aitNo; }
        public String getConfigId() { return configId; }
        public String getDbType() { return dbType; }
        public Map<String, String> getEventMap() { return eventMap; }
    }
}