const state = {
  conversationId: null,
  conversations: [],
  streaming: false,
  source: null,
};

const el = {
  messages: document.querySelector("#messages"),
  form: document.querySelector("#chatForm"),
  input: document.querySelector("#messageInput"),
  send: document.querySelector("#sendButton"),
  cancel: document.querySelector("#cancelConversation"),
  newConversation: document.querySelector("#newConversation"),
  conversationList: document.querySelector("#conversationList"),
  title: document.querySelector("#conversationTitle"),
  modelMeta: document.querySelector("#modelMeta"),
  tabs: document.querySelectorAll(".tab"),
  views: document.querySelectorAll(".view"),
  refreshDashboard: document.querySelector("#refreshDashboard"),
};

function formatTime(ms) {
  return new Date(ms).toLocaleString();
}

function jsonFetch(url, options = {}) {
  return fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  }).then(async (response) => {
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Request failed");
    return data;
  });
}

function setStreaming(value) {
  state.streaming = value;
  el.send.disabled = value;
  el.input.disabled = value;
  el.cancel.disabled = !value || !state.conversationId;
}

function renderEmpty() {
  el.messages.innerHTML = '<div class="empty">Start a conversation. The mock provider works without API keys.</div>';
}

function appendMessage(role, content = "") {
  const node = document.createElement("div");
  node.className = `message ${role}`;
  node.textContent = content;
  el.messages.appendChild(node);
  el.messages.scrollTop = el.messages.scrollHeight;
  return node;
}

function renderMessages(messages) {
  el.messages.innerHTML = "";
  if (!messages.length) {
    renderEmpty();
    return;
  }
  for (const message of messages) {
    appendMessage(message.role, message.content);
  }
}

async function loadConversations() {
  const data = await jsonFetch("/api/conversations");
  state.conversations = data.conversations;
  el.conversationList.innerHTML = "";
  for (const conversation of state.conversations) {
    const button = document.createElement("button");
    button.className = `conversation ${conversation.id === state.conversationId ? "active" : ""}`;
    button.innerHTML = `<strong>${escapeHtml(conversation.title)}</strong><span>${conversation.status} · ${conversation.message_count} messages</span>`;
    button.addEventListener("click", () => loadConversation(conversation.id));
    el.conversationList.appendChild(button);
  }
}

async function loadConversation(id) {
  const data = await jsonFetch(`/api/conversations/${id}`);
  state.conversationId = id;
  el.title.textContent = data.conversation.title;
  el.modelMeta.textContent = `${data.conversation.provider} / ${data.conversation.model} · ${formatTime(data.conversation.updated_at)}`;
  renderMessages(data.messages);
  await loadConversations();
}

async function createConversation() {
  const data = await jsonFetch("/api/conversations", {
    method: "POST",
    body: JSON.stringify({ title: "New conversation" }),
  });
  state.conversationId = data.conversation.id;
  el.title.textContent = data.conversation.title;
  el.modelMeta.textContent = `${data.conversation.provider} / ${data.conversation.model}`;
  renderMessages([]);
  await loadConversations();
}

function sendMessage(message) {
  if (!state.conversationId) {
    return createConversation().then(() => sendMessage(message));
  }
  setStreaming(true);
  if (el.messages.querySelector(".empty")) el.messages.innerHTML = "";
  appendMessage("user", message);
  const assistant = appendMessage("assistant", "");
  const url = `/api/chat/stream?conversation_id=${encodeURIComponent(state.conversationId)}&message=${encodeURIComponent(message)}`;
  const source = new EventSource(url);
  state.source = source;

  source.addEventListener("meta", (event) => {
    const data = JSON.parse(event.data);
    state.conversationId = data.conversation_id;
    el.modelMeta.textContent = `${data.provider} / ${data.model}`;
  });
  source.addEventListener("token", (event) => {
    const data = JSON.parse(event.data);
    assistant.textContent += data.text;
    el.messages.scrollTop = el.messages.scrollHeight;
  });
  source.addEventListener("done", finishStream);
  source.addEventListener("cancelled", finishStream);
  source.addEventListener("failure", (event) => {
    const data = JSON.parse(event.data);
    assistant.textContent = `Error: ${data.error}`;
    finishStream();
  });
}

async function finishStream() {
  if (state.source) state.source.close();
  state.source = null;
  setStreaming(false);
  await loadConversations();
  await loadDashboard();
}

async function cancelConversation() {
  if (!state.conversationId) return;
  await jsonFetch(`/api/conversations/${state.conversationId}/cancel`, { method: "POST", body: "{}" });
}

async function loadDashboard() {
  const data = await jsonFetch("/api/dashboard");
  document.querySelector("#metricRequests").textContent = data.totals.requests || 0;
  document.querySelector("#metricLatency").textContent = `${Math.round(data.totals.avg_latency_ms || 0)} ms`;
  document.querySelector("#metricErrors").textContent = data.totals.errors || 0;
  document.querySelector("#metricTokens").textContent = data.totals.total_tokens || 0;

  document.querySelector("#providerTable").innerHTML = data.by_provider.length
    ? data.by_provider
        .map(
          (row) =>
            `<div class="row"><strong>${escapeHtml(row.provider)}</strong><span>${escapeHtml(row.model)}</span><span>${row.requests} req</span><span>${Math.round(row.avg_latency_ms)} ms</span></div>`,
        )
        .join("")
    : '<p class="empty">No logs yet.</p>';

  const max = Math.max(1, ...data.per_minute.map((row) => row.requests));
  document.querySelector("#throughputBars").innerHTML = data.per_minute.length
    ? data.per_minute.map((row) => `<div class="bar" title="${row.requests} requests" style="height:${Math.max(8, (row.requests / max) * 170)}px"></div>`).join("")
    : '<p class="empty">No traffic yet.</p>';

  document.querySelector("#recentLogs").innerHTML = data.recent.length
    ? data.recent
        .map(
          (row) =>
            `<div class="row"><strong>${escapeHtml(row.status)}</strong><span>${escapeHtml(row.provider)} / ${escapeHtml(row.model)}</span><span>${row.latency_ms} ms</span><span>${formatTime(row.created_at)}</span></div>`,
        )
        .join("")
    : '<p class="empty">No logs yet.</p>';
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[char]);
}

el.form.addEventListener("submit", (event) => {
  event.preventDefault();
  const message = el.input.value.trim();
  if (!message || state.streaming) return;
  el.input.value = "";
  sendMessage(message);
});

el.cancel.addEventListener("click", cancelConversation);
el.newConversation.addEventListener("click", createConversation);
el.refreshDashboard.addEventListener("click", loadDashboard);

for (const tab of el.tabs) {
  tab.addEventListener("click", () => {
    for (const item of el.tabs) item.classList.toggle("active", item === tab);
    for (const view of el.views) view.classList.toggle("active", view.id === tab.dataset.view);
    if (tab.dataset.view === "dashboardView") loadDashboard();
  });
}

createConversation().then(loadDashboard).catch((error) => {
  el.messages.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
});
