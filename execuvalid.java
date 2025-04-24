public void executeAndValidateQuery(TcMaster tcMaster, TcSteps tcStep) {
    log.info("{}#{} updating TC_Execution log to In Progress", tcMaster.getTcId(), tcStep.getStepId());
    long tcExecId = tcMasterServiceHelper.insertLogEntry(tcMaster.getTcId(), tcStep.getStepId(), Constants.INPROGRESS);
    log.info("{}#{} executing and validating query", tcMaster.getTcId(), tcStep.getStepId());

    try {
        if (tcMasterServiceHelper.getAndValidateParametersSchema(tcStep)) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode params = objectMapper.readTree(tcStep.getParameters());

            String query = params.get("query").asText();
            String expectedOutput = params.get("expectedOutput").asText();

            String actualOutput = jdbcTemplate.queryForObject(query, String.class);

            boolean isValid = expectedOutput.equals(actualOutput);

            if (isValid) {
                log.info("{}#{} validation successful", tcMaster.getTcId(), tcStep.getStepId());
                tcMasterServiceHelper.updateLogEntry(tcExecId, Constants.COMPLETED, "Validation passed");
            } else {
                log.info("{}#{} validation failed: Expected={}, Actual={}", 
                         tcMaster.getTcId(), tcStep.getStepId(), expectedOutput, actualOutput);
                tcMasterServiceHelper.updateLogEntry(tcExecId, Constants.FAILED,
                        String.format("Validation failed. Expected: %s, Actual: %s", expectedOutput, actualOutput));
            }

        } else {
            log.info("{}#{} updating TC_Execution log to Failed", tcMaster.getTcId(), tcStep.getStepId());
            tcMasterServiceHelper.updateLogEntry(tcExecId, Constants.FAILED, "Schema validation failed");
            throw new RuntimeException("Schema validation failed");
        }
    } catch (Exception e) {
        log.info("{}#{} updating TC_Execution log to Failed", tcMaster.getTcId(), tcStep.getStepId());
        tcMasterServiceHelper.updateLogEntry(tcExecId, Constants.FAILED, "Error: " + e.getMessage());
        throw new RuntimeException("Error executing and validating query: " + e.getMessage(), e);
    }
}
