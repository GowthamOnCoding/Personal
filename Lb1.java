private List<Stats> mapHostWithProcess(List<Stats> statsList, List<HostStat> hostStatList, String processType) {
    log.info("Sending scheduler info to socket client. Profile: {}", profile);
    log.info("Initial HostStat list size: {}, Stats list size: {}", hostStatList.size(), statsList.size());

    List<Stats> processedStatList = new ArrayList<>();

    do {
        hostStatList = readHostStat(profile, processType);

        if (hostStatList.isEmpty()) {
            log.warn("No eligible hosts found. Retrying after wait...");
            try {
                TimeUnit.SECONDS.sleep(Constants.HOST_REANALYZE_TIME_SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Sleep interrupted while waiting for eligible hosts", e);
                break;
            }
            continue;
        }

        for (Iterator<Stats> statIterator = statsList.iterator(); statIterator.hasNext() && !hostStatList.isEmpty();) {
            Stats stat = statIterator.next();
            HostStat hostStat = hostStatList.remove(0);

            if (Constants.CONSUMER_SET.contains(stat.getProcess())) {
                String groupId = getGroupId(stat);
                stat.setFunnelGroupId(groupId);
            }

            log.info("Assigning stat [{}] to host [{}]", stat.getProcess(), hostStat.getHost());
            socketCommunicator.sendMessage(stat, hostStat);
            processedStatList.add(stat);
            statIterator.remove();

            // Delay to allow host resource stats to reflect the load
            try {
                TimeUnit.SECONDS.sleep(2); // Safe default delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while waiting for host to reflect updated resource usage", e);
                break;
            }

            if (SchedulerApplication.getHookFlag()) {
                log.warn("Hook flag detected. Breaking assignment loop.");
                break;
            }

            // Recheck eligible hosts after each assignment
            hostStatList = readHostStat(profile, processType);

            if (hostStatList.isEmpty()) {
                log.warn("No hosts eligible after reassessment. Breaking inner loop.");
                break;
            }
        }

        if (!statsList.isEmpty() && !SchedulerApplication.getHookFlag()) {
            log.info("Waiting before next host eligibility recheck...");
            try {
                TimeUnit.SECONDS.sleep(Constants.HOST_REANALYZE_TIME_SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Sleep interrupted during reanalyze phase", e);
                break;
            }
        }

    } while (!statsList.isEmpty());

    log.info("Assignment complete. Stats processed: {}", processedStatList.size());
    return processedStatList;
}
