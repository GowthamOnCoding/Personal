public static void migrate(List<Map<String,Object>> rows, Connection dst) throws Exception {
    String cfgSql = "INSERT INTO AIT_CONFIG(AIT_NO, FUNNEL_SDD, AIML_SDD, IDW_SDD, IEDPS_SDD, ESPIAL_SDD, FUNNEL_FFT, IDW_FFT, ESPIAL_FFT, IS_ESPIAL, IS_ACTIVE, AIML_VALIDATION, FULL_SCAN, TOPIC_NAME, AIT_CADENCE, REPORT_TOPIC_NAME, FFT_DESTINATION, PROFILE, LOB, LAST_UPDATED_USER, LAST_UPDATED) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    String dbpSql = "INSERT INTO AIT_DBPROP(AIT_NO, ID, PROFILE, DB_TYPE, MACHINE_NAME, DBA_NAME, SCHEMA_NAME, [USER_ID], PASS_WORD, TOPIC_NAME, JDBC_URL, NO_OF_CONNECTION, EMAIL_ID, IS_ACTIVE, SDD_Active, AIML_IS_ACTIVE, TABLE_LIST, EXEC_STATUS, AIT_CADENCE, IDW_SDD, IDW_UDD, IEDPS_SDD, REPORT_TOPIC_NAME, FUNNEL_UDD, FUNNEL_SDD, AIML_SDD, AIML_UDD, FUNNEL_DESTINATION, FUNNEL_DISCOVERY, AIML_DISCOVERY, IDW_DISCOVERY, IEDPS_DISCOVERY, AIML_VALIDATION, FFT_DESTINATION, AIT_NUM, JDBC_CON_STR, LOB, ENVIRONMENT, MNPI_DISCOVERY, NFQI_DISCOVERY, FULL_SCAN, ONPREM_ID, GUID, COLLECTION_ID, LAST_UPDATED) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    String[] cfgCols = {"AIT_NO","FUNNEL_SDD","AIML_SDD","IDW_SDD","IEDPS_SDD","ESPIAL_SDD","FUNNEL_FFT","IDW_FFT","ESPIAL_FFT","IS_ESPIAL","IS_ACTIVE","AIML_VALIDATION","FULL_SCAN","TOPIC_NAME","AIT_CADENCE","REPORT_TOPIC_NAME","FFT_DESTINATION","PROFILE","LOB","LAST_UPDATED_USER"};
    String[] dbpCols = {"AIT_NO","ID","PROFILE","DB_TYPE","MACHINE_NAME","DBA_NAME","SCHEMA_NAME","USER_ID","PASS_WORD","TOPIC_NAME","JDBC_URL","NO_OF_CONNECTION","EMAIL_ID","IS_ACTIVE","SDD_Active","AIML_IS_ACTIVE","TABLE_LIST","EXEC_STATUS","AIT_CADENCE","IDW_SDD","IDW_UDD","IEDPS_SDD","REPORT_TOPIC_NAME","FUNNEL_UDD","FUNNEL_SDD","AIML_SDD","AIML_UDD","FUNNEL_DESTINATION","FUNNEL_DISCOVERY","AIML_DISCOVERY","IDW_DISCOVERY","IEDPS_DISCOVERY","AIML_VALIDATION","FFT_DESTINATION","AIT_NUM","JDBC_CON_STR","LOB","ENVIRONMENT","MNPI_DISCOVERY","NFQI_DISCOVERY","FULL_SCAN","ONPREM_ID","GUID","COLLECTION_ID"};

    dst.setAutoCommit(false);
    try (PreparedStatement psCfg = dst.prepareStatement(cfgSql);
         PreparedStatement psDbp = dst.prepareStatement(dbpSql)) {

        var seen = new java.util.HashSet<>();
        var now = new java.sql.Timestamp(System.currentTimeMillis());

        for (var r : rows) {
            Object ait = r.get("AIT_NO");

            if (seen.add(ait)) { // only once per AIT_NO
                bind(psCfg, r, cfgCols);
                psCfg.setTimestamp(cfgCols.length+1, now);
                psCfg.addBatch();
            }

            bind(psDbp, r, dbpCols); // all rows
            psDbp.setTimestamp(dbpCols.length+1, now);
            psDbp.addBatch();
        }

        psCfg.executeBatch();
        psDbp.executeBatch();
        dst.commit();
    }
}

private static void bind(PreparedStatement ps, Map<String,Object> row, String[] cols) throws SQLException {
    for (int i=0; i<cols.length; i++) ps.setObject(i+1, row.get(cols[i]));
}
