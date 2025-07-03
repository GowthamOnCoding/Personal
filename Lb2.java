private List<Stats> mapHostWithProcess(List<Stats> statsList, List<HostStat> hostStatList, String processType) {
    log.info("Sending scheduler info to socket client. Profile: {}", profile);
    log.info("Initial Stats list size: {}", statsList.size());

    List<Stats> processedStatList = new ArrayList<>();

    while (!statsList.isEmpty()) {
        hostStatList = readHostStat(profile, processType);

        if (hostStatList.isEmpty()) {
            log.warn("No eligible hosts found. Waiting for 30 seconds before retrying...");
            try {
                TimeUnit.SECONDS.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while waiting for eligible hosts", e);
                break;
            }

            hostStatList = readHostStat(profile, processType);
            if (hostStatList.isEmpty()) {
                log.warn("Still no eligible hosts after wait. Terminating scheduling loop.");
                break;
            }
        }

        Iterator<HostStat> hostIterator = hostStatList.iterator();
        Iterator<Stats> statIterator = statsList.iterator();

        while (hostIterator.hasNext() && statIterator.hasNext()) {
            HostStat hostStat = hostIterator.next();
            Stats stat = statIterator.next();

            if (Constants.CONSUMER_SET.contains(stat.getProcess())) {
                String groupId = getGroupId(stat);
                stat.setFunnelGroupId(groupId);
            }

            log.info("Assigning stat [{}] to host [{}]", stat.getProcess(), hostStat.getHost());
            socketCommunicator.sendMessage(stat, hostStat);

            processedStatList.add(stat);
            hostIterator.remove();
            statIterator.remove();

            // Optional: allow short delay for host to reflect load
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted during post-assignment delay", e);
                break;
            }

            if (SchedulerApplication.getHookFlag()) {
                log.warn("Hook flag triggered. Stopping job assignment.");
                return processedStatList;
            }
        }
    }

    log.info("Assignment complete. Stats processed: {}", processedStatList.size());
    return processedStatList;
}
