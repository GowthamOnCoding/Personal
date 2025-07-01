private List<Stats> mapHostWithProcess(List<Stats> statsList, List<HostStat> hostStatList, String processType) {
    log.info("📡 Sending Scheduler Info to Socket Client... profile: {}", profile);
    log.info("🔢 HostStat List Size: {}, Stats List Size: {}", hostStatList.size(), statsList.size());

    List<Stats> processedStatList = new ArrayList<>();

    do {
        // Re-evaluate eligible hosts
        hostStatList = readHostStat(profile, processType);

        if (hostStatList.isEmpty()) {
            log.info("🚫 No eligible hosts found. Will retry after wait...");
        }

        for (Iterator<Stats> statIterator = statsList.iterator(); statIterator.hasNext() && !hostStatList.isEmpty();) {
            Stats stat = statIterator.next();
            HostStat hostStat = hostStatList.remove(0);  // Assign one host

            // Assign group ID if in CONSUMER_SET
            if (Constants.CONSUMER_SET.contains(stat.getProcess())) {
                String groupId = getGroupId(stat);
                stat.setFunnelGroupId(groupId);
            }

            socketCommunicator.sendMessage(stat, hostStat);
            processedStatList.add(stat);
            statIterator.remove();  // Remove from pending list

            // Exit if hook is triggered
            if (SchedulerApplication.getHookFlag()) {
                log.warn("⚠️ Hook Flag triggered. Breaking assignment loop.");
                break;
            }

            // Re-evaluate host list again (this is optional and costly)
            hostStatList = readHostStat(profile, processType);
        }

        if (!statsList.isEmpty() && !SchedulerApplication.getHookFlag()) {
            try {
                TimeUnit.SECONDS.sleep(Constants.HOST_REANALYZE_TIME_SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("❌ Host reanalyze sleep interrupted", e);
                break;
            }
        }

    } while (!statsList.isEmpty() && !hostStatList.isEmpty());

    log.info("✅ Assignment complete. Processed {} stats.", processedStatList.size());
    return processedStatList;
}
