const POLL_MS = 2000;
const errorBanner = document.getElementById("errorBanner");
const metaEl = document.getElementById("meta");
const kpiRow = document.getElementById("kpiRow");
const cardsEl = document.getElementById("cards");

function timeAgo(iso) {
  if (!iso) return "never";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 2) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

function sparklinePath(values, width, height) {
  if (!values.length) return "";
  const max = Math.max(1, ...values);
  const stepX = values.length > 1 ? width / (values.length - 1) : 0;
  return values
    .map((v, i) => {
      const x = i * stepX;
      const y = height - (v / max) * height;
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

function tile(label, value, unit) {
  return `<div class="tile"><div class="label">${label}</div><div class="value">${value}${unit ? `<span class="unit">${unit}</span>` : ""}</div></div>`;
}

function statusMeta(status) {
  if (status === "busy") return { cls: "busy", text: "Busy" };
  if (status === "down") return { cls: "down", text: "Down" };
  return { cls: "idle", text: "Idle" };
}

function renderCard(svc) {
  const { cls, text } = statusMeta(svc.status);
  const historyMax = Math.max(10, ...svc.lagHistory);
  const meterPct = Math.min(100, (svc.queueDepth / historyMax) * 100);
  const spark = sparklinePath(svc.lagHistory, 220, 32);
  const partitions = svc.partitions.map((p) => `p${p.partition}: ${p.lag}`).join(" · ");

  return `
    <div class="card">
      <div class="card-head">
        <h2>${svc.label}</h2>
        <span class="pill ${cls}"><span class="dot"></span>${text}</span>
      </div>
      <div class="queue-value">
        <span class="n">${svc.queueDepth}</span>
        <span class="label">jobs waiting</span>
      </div>
      <div class="meter-track"><div class="meter-fill" style="width:${meterPct}%"></div></div>
      <div class="stat-row"><span>Throughput</span><strong>${svc.throughputPerSec.toFixed(2)}/s</strong></div>
      <div class="stat-row"><span>Last processed</span><strong>${timeAgo(svc.lastProcessedAt)}</strong></div>
      <div class="stat-row"><span>Consumer group</span><strong>${svc.online ? svc.groupState : "not running"}</strong></div>
      <svg class="sparkline" width="220" height="32" viewBox="0 0 220 32" preserveAspectRatio="none">
        <path d="${spark}" />
      </svg>
      <div class="partitions">${partitions}</div>
    </div>
  `;
}

async function refresh() {
  let data;
  try {
    const res = await fetch("/api/status");
    data = await res.json();
    if (!res.ok) throw new Error(data.error || "request failed");
  } catch (err) {
    errorBanner.style.display = "block";
    errorBanner.textContent = `Visualizer can't reach its data sources: ${err.message}`;
    metaEl.textContent = "stale";
    metaEl.classList.add("stale");
    return;
  }

  if (data.error) {
    errorBanner.style.display = "block";
    errorBanner.textContent = `Partial data — last error: ${data.error}`;
  } else {
    errorBanner.style.display = "none";
  }

  metaEl.classList.remove("stale");
  metaEl.textContent = `updated ${timeAgo(data.timestamp)} · topic "${data.topic.name}" (${data.topic.partitionCount} partitions)`;

  kpiRow.innerHTML = [
    tile("Messages produced", data.topic.producedTotal.toLocaleString(), ""),
    tile("Produce rate", data.topic.producedPerSec.toFixed(2), "/s"),
    tile("Total queue depth", data.totalQueueDepth.toLocaleString(), "jobs"),
  ].join("");

  cardsEl.innerHTML = data.services.map(renderCard).join("");
}

refresh();
setInterval(refresh, POLL_MS);
