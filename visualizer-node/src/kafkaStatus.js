import { Kafka } from "kafkajs";
import { KAFKA_BROKERS, TOPIC_NAME } from "./config.js";

const kafka = new Kafka({ clientId: "visualizer-node", brokers: KAFKA_BROKERS });
const admin = kafka.admin();

let connected = false;

async function ensureConnected() {
  if (connected) return;
  await admin.connect();
  connected = true;
}

// { partition, highWatermark }[] — total messages produced so far, per partition.
export async function fetchTopicOffsets() {
  await ensureConnected();
  const offsets = await admin.fetchTopicOffsets(TOPIC_NAME);
  return offsets.map((o) => ({ partition: o.partition, highWatermark: Number(o.offset) }));
}

// Lag per partition for one consumer group, plus whether the group is actually running.
export async function fetchGroupStatus(groupId, topicOffsets) {
  await ensureConnected();

  let state = "Dead";
  let memberCount = 0;
  try {
    const [description] = (await admin.describeGroups([groupId])).groups;
    state = description.state;
    memberCount = description.members.length;
  } catch {
    // group has never been created (service never started) — treat as down.
  }

  let committedByPartition = new Map();
  try {
    const [topicOffset] = await admin.fetchOffsets({ groupId, topics: [TOPIC_NAME] });
    for (const p of topicOffset.partitions) {
      committedByPartition.set(p.partition, Number(p.offset));
    }
  } catch {
    // no committed offsets yet.
  }

  const partitions = topicOffsets.map(({ partition, highWatermark }) => {
    const committedRaw = committedByPartition.get(partition);
    const committed = committedRaw === undefined || committedRaw < 0 ? 0 : committedRaw;
    return { partition, highWatermark, committed, lag: Math.max(0, highWatermark - committed) };
  });

  const totalLag = partitions.reduce((sum, p) => sum + p.lag, 0);
  const online = memberCount > 0 && (state === "Stable" || state === "PreparingRebalance" || state === "CompletingRebalance");

  return { state, memberCount, online, totalLag, partitions };
}

export async function disconnect() {
  if (connected) await admin.disconnect();
}
