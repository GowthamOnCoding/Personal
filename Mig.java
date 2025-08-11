import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AitMigrator {

    // === ENTRYPOINT ===
    public static void migrate(
            List<Map<String,Object>> sourceRows, Connection dst,
            String cfgTable, String dbpTable
    ) throws Exception {
        if (sourceRows == null || sourceRows.isEmpty()) return;

        // 1) Build explicit mappings (DEST COL -> value from row/default)
        LinkedHashMap<String, Function<Map<String,Object>, Object>> cfgMap = buildCfgMap();
        LinkedHashMap<String, Function<Map<String,Object>, Object>> dbpMap = buildDbpMap();

        // 2) Unique AIT list to wipe first
        Set<Object> aits = sourceRows.stream().map(r -> r.get("AIT_NO")).collect(Collectors.toSet());

        dst.setAutoCommit(false);
        try {
            deleteByAits(dst, cfgTable, aits);
            deleteByAits(dst, dbpTable, aits);

            String cfgSql = buildInsertSql(cfgTable, cfgMap.keySet(), "LAST_UPDATED");
            String dbpSql = buildInsertSql(dbpTable, dbpMap.keySet(), "LAST_UPDATED");

            try (PreparedStatement psCfg = dst.prepareStatement(cfgSql);
                 PreparedStatement psDbp = dst.prepareStatement(dbpSql)) {

                Timestamp now = new Timestamp(System.currentTimeMillis());
                Set<Object> seen = new HashSet<>();

                for (Map<String,Object> row : sourceRows) {
                    Object ait = row.get("AIT_NO");

                    // AIT_CONFIG: once per AIT
                    if (seen.add(ait)) {
                        bind(psCfg, row, cfgMap);
                        psCfg.setTimestamp(cfgMap.size()+1, now);
                        psCfg.addBatch();
                    }
                    // AIT_DBPROP: every row
                    bind(psDbp, row, dbpMap);
                    psDbp.setTimestamp(dbpMap.size()+1, now);
                    psDbp.addBatch();
                }
                psCfg.executeBatch();
                psDbp.executeBatch();
            }
            dst.commit();
        } catch (Exception e) {
            dst.rollback();
            throw e;
        } finally {
            dst.setAutoCommit(true);
        }
    }

    // === DEST → VALUE mappings ===
    private static LinkedHashMap<String, Function<Map<String,Object>, Object>> buildCfgMap() {
        LinkedHashMap<String, Function<Map<String,Object>, Object>> m = new LinkedHashMap<>();
        // present in source
        m.put("AIT_NO",            r -> r.get("AIT_NO"));
        m.put("FUNNEL_SDD",        r -> asInt(r.get("FUNNEL_SDD")));
        m.put("AIML_SDD",          r -> asInt(r.get("AIML_SDD")));
        m.put("IDW_SDD",           r -> asInt(r.get("IDW_SDD")));
        m.put("IEDPS_SDD",         r -> asInt(r.get("IEDPS_SDD")));
        m.put("AIML_VALIDATION",   r -> asBool(r.get("AIML_VALIDATION")));
        m.put("FULL_SCAN",         r -> asBool(r.get("FULL_SCAN"))); // if missing, helper returns null -> default below
        m.put("TOPIC_NAME",        r -> trim(r.get("TOPIC_NAME")));
        m.put("AIT_CADENCE",       r -> r.get("AIT_CADENCE"));
        m.put("REPORT_TOPIC_NAME", r -> r.get("REPORT_TOPIC_NAME"));
        m.put("FFT_DESTINATION",   r -> r.get("FFT_DESTINATION"));
        m.put("PROFILE",           r -> r.get("PROFILE"));
        m.put("LOB",               r -> r.get("LOB"));
        m.put("LAST_UPDATED_USER", r -> r.getOrDefault("LAST_UPDATED_USER","system"));

        // not in source → defaults
        m.put("ESPIAL_SDD",        r -> 0);
        m.put("FUNNEL_FFT",        r -> 0);
        m.put("IDW_FFT",           r -> 0);
        m.put("ESPIAL_FFT",        r -> 0);
        m.put("IS_ESPIAL",         r -> 0);      // or false
        m.put("IS_ACTIVE",         r -> 1);      // default active (bit/int)
        // note: LAST_UPDATED is appended automatically
        return m;
    }

    private static LinkedHashMap<String, Function<Map<String,Object>, Object>> buildDbpMap() {
        LinkedHashMap<String, Function<Map<String,Object>, Object>> m = new LinkedHashMap<>();
        // mostly present in source
        m.put("AIT_NO",            r -> r.get("AIT_NO"));
        m.put("ID",                r -> r.get("ID"));
        m.put("PROFILE",           r -> r.get("PROFILE"));
        m.put("DB_TYPE",           r -> r.get("DB_TYPE"));
        m.put("MACHINE_NAME",      r -> r.get("MACHINE_NAME"));
        m.put("DBA_NAME",          r -> r.get("DBA_NAME"));
        m.put("SCHEMA_NAME",       r -> r.get("SCHEMA_NAME"));
        m.put("USER_ID",           r -> r.get("USER_ID"));
        m.put("PASS_WORD",         r -> r.get("PASS_WORD"));
        m.put("TOPIC_NAME",        r -> trim(r.get("TOPIC_NAME")));
        m.put("JDBC_URL",          r -> r.get("JDBC_URL"));
        m.put("NO_OF_CONNECTION",  r -> asInt(r.get("NO_OF_CONNECTION")));
        m.put("EMAIL_ID",          r -> r.get("EMAIL_ID"));
        m.put("SDD_Active",        r -> r.get("SDD_Active"));
        m.put("AIML_IS_ACTIVE",    r -> asBool(r.get("AIML_IS_ACTIVE")));
        m.put("TABLE_LIST",        r -> r.get("TABLE_LIST"));
        m.put("EXEC_STATUS",       r -> r.get("EXEC_STATUS"));
        m.put("AIT_CADENCE",       r -> r.get("AIT_CADENCE"));
        m.put("IDW_SDD",           r -> asInt(r.get("IDW_SDD")));
        m.put("IDW_UDD",           r -> asInt(r.get("IDW_UDD")));
        m.put("IEDPS_SDD",         r -> asInt(r.get("IEDPS_SDD")));
        m.put("REPORT_TOPIC_NAME", r -> r.get("REPORT_TOPIC_NAME"));
        m.put("FUNNEL_UDD",        r -> asInt(r.get("FUNNEL_UDD")));
        m.put("FUNNEL_SDD",        r -> asInt(r.get("FUNNEL_SDD")));
        m.put("AIML_SDD",          r -> asInt(r.get("AIML_SDD")));
        m.put("AIML_UDD",          r -> asInt(r.get("AIML_UDD")));
        m.put("FUNNEL_DESTINATION",r -> r.get("FUNNEL_DESTINATION"));
        m.put("FUNNEL_DISCOVERY",  r -> asBool(r.get("FUNNEL_DISCOVERY")));
        m.put("AIML_DISCOVERY",    r -> asBool(r.get("AIML_DISCOVERY")));
        m.put("IDW_DISCOVERY",     r -> asBool(r.get("IDW_DISCOVERY")));
        m.put("IEDPS_DISCOVERY",   r -> asBool(r.get("IEDPS_DISCOVERY")));
        m.put("AIML_VALIDATION",   r -> asBool(r.get("AIML_VALIDATION")));
        m.put("FFT_DESTINATION",   r -> r.get("FFT_DESTINATION"));
        m.put("AIT_NUM",           r -> r.get("AIT_NUM"));
        m.put("JDBC_CON_STR",      r -> r.get("JDBC_CON_STR"));
        m.put("LOB",               r -> r.get("LOB"));
        m.put("ENVIRONMENT",       r -> r.get("ENVIRONMENT"));
        m.put("MNPI_DISCOVERY",    r -> asBool(r.get("MNPI_DISCOVERY")));
        m.put("NFQI_DISCOVERY",    r -> asBool(r.get("NFQI_DISCOVERY")));

        // not in source → defaults
        m.put("FULL_SCAN",         r -> 0);
        m.put("IS_ACTIVE",         r -> 1);
        m.put("ONPREM_ID",         r -> null);
        m.put("GUID",              r -> java.util.UUID.randomUUID().toString());
        m.put("COLLECTION_ID",     r -> null);
        // LAST_UPDATED appended automatically
        return m;
    }

    // === SQL & bind helpers ===
    private static void deleteByAits(Connection c, String table, Set<Object> aits) throws SQLException {
        if (aits.isEmpty()) return;
        String q = String.join(",", Collections.nCopies(aits.size(), "?"));
        String sql = "DELETE FROM " + table + " WHERE AIT_NO IN (" + q + ")";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int i=1; for (Object v: aits) ps.setObject(i++, v);
            ps.executeUpdate();
        }
    }

    private static String buildInsertSql(String table, Collection<String> cols, String extraCol) {
        String colList = String.join(",", cols) + "," + extraCol;
        String qs = String.join(",", Collections.nCopies(cols.size()+1, "?"));
        return "INSERT INTO " + table + " (" + colList + ") VALUES (" + qs + ")";
    }

    private static void bind(PreparedStatement ps,
                             Map<String,Object> row,
                             LinkedHashMap<String, Function<Map<String,Object>, Object>> mapping) throws SQLException {
        int i = 1;
        for (var fn : mapping.values()) {
            ps.setObject(i++, fn.apply(row));
        }
    }

    // === tiny coercion helpers ===
    private static Integer asInt(Object o){ if(o==null) return null; if(o instanceof Number n) return n.intValue(); return Integer.valueOf(String.valueOf(o).trim()); }
    private static Boolean asBool(Object o){
        if(o==null) return null;
        if(o instanceof Boolean b) return b;
        if(o instanceof Number n) return n.intValue()!=0;
        String s = String.valueOf(o).trim();
        return "Y".equalsIgnoreCase(s) || "YES".equalsIgnoreCase(s) || "TRUE".equalsIgnoreCase(s) || "1".equals(s);
    }
    private static String trim(Object o){ return o==null? null : String.valueOf(o).trim(); }
}
