import { jsxs as _jsxs, jsx as _jsx } from "react/jsx-runtime";
import { useState } from 'react';
import { VaultSyncProvider, useQuery, useVaultSyncClient, useSyncStatus } from '@vaultsync/react';
function TodoApp({ replicaName }) {
    const client = useVaultSyncClient();
    const { data: todos, loading } = useQuery('todos');
    const status = useSyncStatus();
    const [text, setText] = useState('');
    const addTodo = async (e) => {
        e.preventDefault();
        if (!text.trim())
            return;
        const todoId = `todo-${Date.now()}`;
        await client.insert('todos', todoId, {
            text: text.trim(),
            completed: false,
        });
        setText('');
    };
    const deleteTodo = async (todoId) => {
        await client.delete('todos', todoId);
    };
    return (_jsxs("div", { className: "panel", children: [_jsxs("h2", { children: ["Replica ", replicaName] }), _jsxs("div", { className: "status-bar", children: [_jsx("div", { className: `status-dot ${status.connected ? 'connected' : 'disconnected'}` }), _jsx("span", { children: status.connected ? 'Connected' : 'Offline' }), _jsxs("span", { style: { marginLeft: 'auto', fontSize: '0.9rem', opacity: 0.7 }, children: ["Pending: ", status.pendingMutations] })] }), _jsxs("form", { onSubmit: addTodo, className: "input-group", children: [_jsx("input", { type: "text", placeholder: "What needs to be done?", value: text, onChange: (e) => setText(e.target.value) }), _jsx("button", { type: "submit", children: "Add" })] }), loading ? (_jsx("div", { children: "Loading todos..." })) : (_jsx("ul", { className: "todo-list", children: todos.map((todo) => (_jsxs("li", { className: "todo-item", children: [_jsx("span", { className: "todo-text", children: todo.text }), _jsx("button", { className: "todo-delete", onClick: () => deleteTodo(todo.record_id), children: "Delete" })] }, todo.record_id || String(Math.random())))) }))] }));
}
export default function App() {
    const configA = {
        namespace: 'collab-demo',
        replicaId: 'replica-A',
        coordinatorUrl: 'http://127.0.0.1:8080',
    };
    const configB = {
        namespace: 'collab-demo',
        replicaId: 'replica-B',
        coordinatorUrl: 'http://127.0.0.1:8080',
    };
    return (_jsxs("div", { className: "container", children: [_jsx("h1", { children: "VaultSync Collaborative Sync Demo" }), _jsx("p", { style: { opacity: 0.8, marginBottom: '2rem' }, children: "This demo spawns two independent sync engine instances on the same page. Ensure the VaultSync Coordinator Server is running on port 8080 (`cargo run -p vaultsync-coordinator-server -- -p 8080`)." }), _jsxs("div", { className: "grid", children: [_jsx(VaultSyncProvider, { config: configA, children: _jsx(TodoApp, { replicaName: "A" }) }), _jsx(VaultSyncProvider, { config: configB, children: _jsx(TodoApp, { replicaName: "B" }) })] })] }));
}
//# sourceMappingURL=App.js.map