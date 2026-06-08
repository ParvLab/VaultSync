import { DriftClient } from "@drift/web"

declare global { interface Window { __DRIFT__: any } }

const urlParams = new URLSearchParams(window.location.search);
const namespace = urlParams.get("ns") || ("e2e-test-" + Date.now());
const replicaId = "replica-" + Math.random().toString(36).substring(7);

const client = await DriftClient.create({
  namespace,
  replicaId,
  coordinatorUrl: "http://localhost:9876",
})

window.__DRIFT__ = {
  client,
  insert: (id: string, title: string) =>
    client.insert("todos", id, { id, title, done: false }),
  update: (id: string, fields: any) =>
    client.update("todos", id, fields),
  delete: (id: string) =>
    client.delete("todos", id),
  findAll: () => client.find("todos"),
  syncStatus: () => client.syncStatus(),
}

// Subscribe to updates and render list
const renderTodos = async () => {
  const todos = await client.find("todos");
  const list = document.getElementById("todo-list")!;
  list.innerHTML = todos.map(t =>
    `<li data-testid="todo-item" data-id="${t.id}">${t.title}</li>`
  ).join("")
}

client.subscribe("todos", () => {
  renderTodos();
})

// Initial render
renderTodos();

// Periodically update connection status
setInterval(async () => {
  try {
    const status = await client.syncStatus();
    const statusEl = document.getElementById("sync-status")!;
    statusEl.textContent = status.connected ? "connected" : "disconnected";
    statusEl.setAttribute("data-status", status.connected ? "connected" : "disconnected");
  } catch (e) {
    // ignore
  }
}, 500);

document.getElementById("todo-form")?.addEventListener("submit", async (e) => {
  e.preventDefault()
  const input = document.getElementById("todo-input") as HTMLInputElement
  const id = Math.random().toString(36).substring(7);
  await client.insert("todos", id, { id, title: input.value, done: false })
  input.value = ""
  renderTodos();
})
