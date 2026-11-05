import { fetchTopicOffsets, fetchGroupStatus } from "./kafkaStatus.js";
import { fetchTableActivity } from "./db.js";
import { SERVICES, HISTORY_LENGTH, RECENT_WINDOW_SECONDS, TOPIC_NAME } from "./config.js";

// In-memory only — a restart clears history, which is fine for an ops view.
const history = {
  producedTotal: [],
  services: Object.fromEntries(SERVICES.map((s) => [s.id, { lag: [], throughput: [] }])),
};

let lastProducedTotal = null;
let lastProducedAt = null;

function pushCapped(arr, value) {
  arr.push(value);
  if (arr.length > HISTORY_LENGTH) arr.shift();
}

function classify({ online, lag, recentDbWrites }) {
  if (!online) return "down";
  if (lag > 0 || recentDbWrites > 0) return "busy";
  return "idle";
}

export async function buildSnapshot() {
  const now = Date.now();
  const topicOffsets = await fetchTopicOffsets();
  const producedTotal = topicOffsets.reduce((sum, p) => sum + p.highWatermark, 0);

  let producedPerSec = 0;
  if (lastProducedTotal !== null && lastProducedAt !== null) {
    const seconds = (now - lastProducedAt) / 1000;
    producedPerSec = seconds > 0 ? Math.max(0, (producedTotal - lastProducedTotal) / seconds) : 0;
  }
  lastProducedTotal = producedTotal;
  lastProducedAt = now;
  pushCapped(history.producedTotal, producedTotal);

  const services = await Promise.all(
    SERVICES.map(async (svc) => {
      const [group, dbActivity] = await Promise.all([
        fetchGroupStatus(svc.groupId, topicOffsets),
        fetchTableActivity(svc.resultsTable, RECENT_WINDOW_SECONDS).catch(() => null),
      ]);

      const recentCount = dbActivity ? dbActivity.recent_count : 0;
      const throughputPerSec = recentCount / RECENT_WINDOW_SECONDS;
      const status = classify({ online: group.online, lag: group.totalLag, recentDbWrites: recentCount });

      const h = history.services[svc.id];
      pushCapped(h.lag, group.totalLag);
      pushCapped(h.throughput, throughputPerSec);

      return {
        id: svc.id,
        label: svc.label,
        groupId: svc.groupId,
        status,
        online: group.online,
        groupState: group.state,
        memberCount: group.memberCount,
        queueDepth: group.totalLag,
        partitions: group.partitions,
        throughputPerSec,
        totalProcessed: dbActivity ? dbActivity.total_count : null,
        lastProcessedAt: dbActivity ? dbActivity.last_processed_at : null,
        lagHistory: h.lag,
        throughputHistory: h.throughput,
      };
    })
  );

  return {
    timestamp: new Date(now).toISOString(),
    topic: {
      name: TOPIC_NAME,
      partitionCount: topicOffsets.length,
      producedTotal,
      producedPerSec,
      producedHistory: history.producedTotal,
    },
    services,
    totalQueueDepth: services.reduce((sum, s) => sum + s.queueDepth, 0),
  };
}
