function renderBarChart(el, rows, labelKey, countKey) {
  const max = Math.max(1, ...rows.map((r) => r[countKey]));
  el.innerHTML = rows
    .map(
      (r) => `
      <div class="bar-row">
        <div class="bar-label">${r[labelKey]}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${(r[countKey] / max) * 100}%"></div></div>
        <div class="bar-count">${r[countKey]}</div>
      </div>`
    )
    .join("");
}

function sentimentPillClass(label) {
  const l = (label || "").toLowerCase();
  if (l.includes("pos")) return "pos";
  if (l.includes("neg")) return "neg";
  return "neu";
}

function entityPillClass(label) {
  const l = (label || "").toLowerCase();
  if (l === "per") return "ent-per";
  if (l === "org") return "ent-org";
  if (l === "loc") return "ent-loc";
  return "ent-misc";
}

function renderEntityPills(entities) {
  if (!entities || entities.length === 0) return "-";
  return entities
    .slice(0, 6)
    .map((e) => `<span class="pill ${entityPillClass(e.label)}">${e.text} <small>${e.label}</small></span>`)
    .join(" ");
}

async function loadDashboard() {
  const [messages, sentiment, topics, toxicity, ner] = await Promise.all([
    fetch("/api/messages?limit=50").then((r) => r.json()),
    fetch("/api/stats/sentiment").then((r) => r.json()),
    fetch("/api/stats/topics").then((r) => r.json()),
    fetch("/api/stats/toxicity").then((r) => r.json()),
    fetch("/api/stats/ner").then((r) => r.json()),
  ]);

  renderBarChart(document.getElementById("sentimentChart"), sentiment, "label", "count");
  renderBarChart(document.getElementById("topicChart"), topics, "label", "count");
  renderBarChart(
    document.getElementById("toxicityChart"),
    toxicity.map((t) => ({ label: t.is_toxic ? "toxic" : "clean", count: t.count })),
    "label",
    "count"
  );
  renderBarChart(document.getElementById("nerChart"), ner, "label", "count");

  document.getElementById("messagesBody").innerHTML = messages
    .map(
      (m) => `
      <tr>
        <td class="text">${(m.text || "").slice(0, 200)}</td>
        <td><span class="pill ${sentimentPillClass(m.sentiment_label)}">${m.sentiment_label ?? "-"}</span></td>
        <td>${m.topic_label ?? "-"}</td>
        <td><span class="pill ${m.is_toxic ? "toxic" : "clean"}">${m.is_toxic === null ? "-" : m.is_toxic ? "toxic" : "clean"}</span></td>
        <td>${renderEntityPills(m.ner_entities)}</td>
      </tr>`
    )
    .join("");

  document.getElementById("refreshedAt").textContent = `updated ${new Date().toLocaleTimeString()}`;
}

loadDashboard();
setInterval(loadDashboard, 5000);
