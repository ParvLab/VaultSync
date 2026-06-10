import React, { useState, useEffect } from 'react';
import { useQuery, useDriftMutations } from '@drift/react';
import { Drift } from '@drift/web';

export function App({ replicaId, drift }: { replicaId: string; drift: Drift }) {
  const [latency, setLatency] = useState(0); // in ms
  const [packetLoss, setPacketLoss] = useState(0); // percentage
  
  // Simulated replica states
  const [clientAVal, setClientAVal] = useState('');
  const [clientBVal, setClientBVal] = useState('');
  
  // Conflict log
  const [logs, setLogs] = useState<string[]>([]);

  // Query shared doc
  const { data: sharedItems } = useQuery('shared');
  const mutations = useDriftMutations('shared');

  const addLog = (msg: string) => {
    setLogs((prev) => [`[${new Date().toLocaleTimeString()}] ${msg}`, ...prev].slice(0, 10));
  };

  const handleClientAEdit = async () => {
    addLog(`Client A updates 'title' -> "${clientAVal}" with ${latency}ms latency`);
    setTimeout(async () => {
      if (Math.random() * 100 < packetLoss) {
        addLog(`❌ Client A update dropped (Simulated Packet Loss)`);
        return;
      }
      await mutations.insert('shared-doc', {
        title: clientAVal,
      });
      addLog(`✓ Client A update saved & merged`);
    }, latency);
  };

  const handleClientBEdit = async () => {
    addLog(`Client B updates 'title' -> "${clientBVal}" with ${latency}ms latency`);
    setTimeout(async () => {
      if (Math.random() * 100 < packetLoss) {
        addLog(`❌ Client B update dropped (Simulated Packet Loss)`);
        return;
      }
      await mutations.insert('shared-doc', {
        title: clientBVal,
      });
      addLog(`✓ Client B update saved & merged`);
    }, latency);
  };

  const doc = sharedItems.find((x: any) => x.id === 'shared-doc') || { title: '(None)' };

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1>Drift Chaos Testing Simulator</h1>
        <p style={styles.subtitle}>Simulating latency, packet loss, and concurrent edits</p>
      </header>

      <div style={styles.grid}>
        <div style={styles.card}>
          <h2>Network Fault Injector</h2>
          
          <div style={styles.controlGroup}>
            <label style={styles.label}>Simulated Latency: {latency}ms</label>
            <input
              type="range"
              min="0"
              max="3000"
              step="100"
              value={latency}
              onChange={(e) => setLatency(Number(e.target.value))}
              style={styles.slider}
            />
          </div>

          <div style={styles.controlGroup}>
            <label style={styles.label}>Simulated Packet Loss: {packetLoss}%</label>
            <input
              type="range"
              min="0"
              max="100"
              step="5"
              value={packetLoss}
              onChange={(e) => setPacketLoss(Number(e.target.value))}
              style={styles.slider}
            />
          </div>
        </div>

        <div style={styles.card}>
          <h2>Shared Document State</h2>
          <div style={styles.stateDisplay}>
            <span style={styles.stateLabel}>Current Converged Value:</span>
            <strong style={styles.stateVal}>{doc.title}</strong>
          </div>
          <p style={styles.helperText}>
            LWW-Element-Set CRDT resolver automatically merges updates using Lamport timestamps, ensuring eventual consistency.
          </p>
        </div>

        <div style={styles.card}>
          <h2>Client A Workspace</h2>
          <div style={styles.form}>
            <input
              type="text"
              placeholder="Edit title..."
              value={clientAVal}
              onChange={(e) => setClientAVal(e.target.value)}
              style={styles.input}
            />
            <button onClick={handleClientAEdit} style={styles.clientBtn}>Send Update</button>
          </div>
        </div>

        <div style={styles.card}>
          <h2>Client B Workspace</h2>
          <div style={styles.form}>
            <input
              type="text"
              placeholder="Edit title..."
              value={clientBVal}
              onChange={(e) => setClientBVal(e.target.value)}
              style={styles.input}
            />
            <button onClick={handleClientBEdit} style={styles.clientBtn}>Send Update</button>
          </div>
        </div>

        <div style={styles.cardFull}>
          <h2>Real-time Chaos logs</h2>
          <div style={styles.logContainer}>
            {logs.length === 0 ? (
              <p style={styles.emptyLog}>No logs yet. Perform some edits above to trigger simulated faults.</p>
            ) : (
              logs.map((log, i) => (
                <div key={i} style={styles.logItem}>{log}</div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    maxWidth: '900px',
    margin: '40px auto',
    padding: '24px',
    borderRadius: '16px',
    backgroundColor: '#0f172a',
    color: '#f8fafc',
    fontFamily: 'Inter, system-ui, sans-serif',
  },
  header: {
    borderBottom: '1px solid #1e293b',
    paddingBottom: '16px',
    marginBottom: '24px',
  },
  subtitle: {
    color: '#64748b',
    margin: '4px 0 0 0',
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
    gap: '20px',
  },
  card: {
    backgroundColor: '#1e293b',
    padding: '20px',
    borderRadius: '12px',
    border: '1px solid #334155',
  },
  cardFull: {
    gridColumn: '1 / span 2',
    backgroundColor: '#1e293b',
    padding: '20px',
    borderRadius: '12px',
    border: '1px solid #334155',
  },
  controlGroup: {
    marginBottom: '16px',
  },
  label: {
    display: 'block',
    fontSize: '13px',
    color: '#94a3b8',
    marginBottom: '8px',
    fontWeight: '600',
  },
  slider: {
    width: '100%',
    cursor: 'pointer',
  },
  stateDisplay: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '24px',
    backgroundColor: '#0f172a',
    borderRadius: '8px',
    marginBottom: '12px',
  },
  stateLabel: {
    fontSize: '12px',
    color: '#64748b',
    textTransform: 'uppercase',
  },
  stateVal: {
    fontSize: '24px',
    color: '#38bdf8',
    marginTop: '6px',
  },
  helperText: {
    fontSize: '11px',
    color: '#64748b',
    margin: 0,
    lineHeight: '1.4',
  },
  form: {
    display: 'flex',
    gap: '12px',
  },
  input: {
    flex: 1,
    padding: '10px 12px',
    borderRadius: '8px',
    border: '1px solid #334155',
    backgroundColor: '#0f172a',
    color: '#f8fafc',
    outline: 'none',
  },
  clientBtn: {
    padding: '10px 16px',
    backgroundColor: '#0284c7',
    border: 'none',
    borderRadius: '8px',
    color: '#fff',
    fontWeight: 'bold',
    cursor: 'pointer',
  },
  logContainer: {
    maxHeight: '160px',
    overflowY: 'auto',
    backgroundColor: '#0f172a',
    padding: '12px',
    borderRadius: '8px',
    fontFamily: 'monospace',
    fontSize: '12px',
  },
  emptyLog: {
    color: '#64748b',
    textAlign: 'center',
    margin: '20px 0',
  },
  logItem: {
    padding: '4px 0',
    borderBottom: '1px solid #1e293b',
    color: '#38bdf8',
  },
};
