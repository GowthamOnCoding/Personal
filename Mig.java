public static void migrateDynamicWithDeleteTx(
        List<Map<String,Object>> rows,
        Connection dst,
        String cfgTable,
        String dbpTable
) throws Exception {

    if (rows.isEmpty()) return;

    // Collect all unique AIT_NO values
    var aitSet = new java.util.HashSet<>();
    for (Map<String,Object> r : rows) {
        aitSet.add(r.get("AIT_NO"));
    }

    dst.setAutoCommit(false); // start transaction
    try (
        PreparedStatement delCfg = dst.prepareStatement(
            "DELETE FROM " + cfgTable + " WHERE AIT_NO = ?");
        PreparedStatement delDbp = dst.prepareStatement(
            "DELETE FROM " + dbpTable + " WHERE AIT_NO = ?")
    ) {
        // --- Step 1: Delete existing rows for these AITs ---
        for (Object ait : aitSet) {
            delCfg.setObject(1, ait);
            delCfg.addBatch();

            delDbp.setObject(1, ait);
            delDbp.addBatch();
        }
        delCfg.executeBatch();
        delDbp.executeBatch();

        // --- Step 2: Build INSERT statements dynamically ---
        String[] cfgCols = rows.get(0).keySet().toArray(String[]::new);
        String cfgSql = buildInsertSql(cfgTable, cfgCols, "LAST_UPDATED");

        String[] dbpCols = rows.get(0).keySet().toArray(String[]::new);
        String dbpSql = buildInsertSql(dbpTable, dbpCols, "LAST_UPDATED");

        // --- Step 3: Insert ---
        try (PreparedStatement psCfg = dst.prepareStatement(cfgSql);
             PreparedStatement psDbp = dst.prepareStatement(dbpSql)) {

            var seenAit = new java.util.HashSet<>();
            var now = new java.sql.Timestamp(System.currentTimeMillis());

            for (Map<String,Object> row : rows) {
                Object ait = row.get("AIT_NO");

                // CONFIG: only insert once per AIT_NO
                if (seenAit.add(ait)) {
                    bind(psCfg, row, cfgCols);
                    psCfg.setTimestamp(cfgCols.length+1, now);
                    psCfg.addBatch();
                }

                // DBPROP: insert all rows
                bind(psDbp, row, dbpCols);
                psDbp.setTimestamp(dbpCols.length+1, now);
                psDbp.addBatch();
            }

            psCfg.executeBatch();
            psDbp.executeBatch();
        }

        dst.commit(); // commit all changes
    } catch (Exception e) {
        dst.rollback(); // rollback everything if something fails
        throw e;
    } finally {
        dst.setAutoCommit(true); // restore default
    }
}

// Build INSERT SQL dynamically
private static String buildInsertSql(String table, String[] cols, String extraCol) {
    String colList = String.join(",", cols) + "," + extraCol;
    String placeholders = String.join(",", java.util.Collections.nCopies(cols.length+1, "?"));
    return "INSERT INTO " + table + " (" + colList + ") VALUES (" + placeholders + ")";
}

// Bind Map values to PreparedStatement
private static void bind(PreparedStatement ps, Map<String,Object> row, String[] cols) throws SQLException {
    for (int i = 0; i < cols.length; i++) {
        ps.setObject(i + 1, row.get(cols[i]));
    }
}
