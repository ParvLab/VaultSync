import React, { useState, useEffect } from 'react';
import { useQuery, useDriftMutations } from '@drift/react';
import { Drift, CustomCoordinator } from '@drift/web';

export function App({ replicaId, drift }: { replicaId: string; drift: Drift }) {
  const [authToken, setAuthToken] = useState('secret-token-123');
  const [headers, setHeaders] = useState('{"X-Client-Version": "1.0.0"}');
  
  // Custom coordinator state
  const [coordinator, setCoordinator] = useState<CustomCoordinator | null>(null);

  // Key manager state
  const [keys, setKeys] = useState<any[]>([]);
  const [activeVersion, setActiveVersion] = useState(0);

  // Setup custom coordinator client config
  useEffect(() => {
    try {
      const parsedHeaders = JSON.parse(headers);
      const custom = new CustomCoordinator({
        url: 'ws://127.0.0.1:8080/sync',
        authToken,
        headers: parsedHeaders,
      });
      setCoordinator(custom);
    } catch (e) {
      // Ignore JSON parse errors while typing headers
    }
  }, [authToken, headers]);

  // Load keyring info
  const loadKeyring = async () => {
    const list = await drift.keys.listVersions();
    setKeys(list);
    setActiveVersion(drift.keys.activeVersion());
  };

  useEffect(() => {
    loadKeyring();
  }, [drift]);

  const handleRotateKey = async () => {
    await drift.keys.rotate();
    await loadKeyring();
  };

  const handleDefineSchema = async () => {
    // Define a custom schema for 'posts' doc
    await (drift as any).client.defineSchema('posts', JSON.stringify({
      fields: {
        title: { type: 'String' },
        likes: { type: 'Number' },
        published: { type: 'Boolean' },
      }
    }));
    alert('Defined schema validation for "posts" successfully!');
  };

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1>Drift Custom Coordinator</h1>
        <p style={styles.subtitle}>Fine-grained coordinator settings & key management</p>
      </header>

      <div style={styles.grid}>
        <div style={styles.card}>
          <h2>Coordinator Configuration</h2>
          <div style={styles.formGroup}>
            <label style={styles.label}>Auth Token</label>
            <input
              type="text"
              value={authToken}
              onChange={(e) => setAuthToken(e.target.value)}
              style={styles.input}
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.label}>Custom Headers (JSON)</label>
            <textarea
              value={headers}
              onChange={(e) => setHeaders(e.target.value)}
              style={{ ...styles.input, height: '80px', fontFamily: 'monospace' }}
            />
          </div>
          {coordinator && (
            <div style={styles.configOutput}>
              <p><strong>Configured URL:</strong> {coordinator.url}</p>
              <p><strong>Auth Token:</strong> {coordinator.authToken ? '✓ Present' : '❌ None'}</p>
              <p><strong>Headers:</strong> {JSON.stringify(coordinator.headers)}</p>
            </div>
          )}
        </div>

        <div style={styles.card}>
          <h2>Key Management (E2EE)</h2>
          <p style={styles.cardText}>
            Drift encrypts all mutations before sending them to the coordinator. You can rotate keys locally at any time.
          </p>
          <div style={styles.keyRow}>
            <span>Active Key Version:</span>
            <strong style={styles.highlight}>v{activeVersion}</strong>
          </div>
          <button onClick={handleRotateKey} style={styles.primaryButton}>
            Rotate Encryption Keys
          </button>
          
          <div style={styles.keyList}>
            <h3>Key History</h3>
            {keys.map((k) => (
              <div key={k.version} style={styles.keyItem}>
                <span>Version {k.version}</span>
                <span style={{
                  color: k.isActive ? '#10b981' : '#9ca3af',
                  fontWeight: k.isActive ? 'bold' : 'normal',
                }}>
                  {k.isActive ? 'Active' : 'Retired'}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div style={styles.cardFull}>
          <h2>Schema Registry Definition</h2>
          <p style={styles.cardText}>
            You can define strict types for collections. The local engine will reject insert/updates violating the schema.
          </p>
          <button onClick={handleDefineSchema} style={styles.secondaryButton}>
            Define "posts" Collection Schema
          </button>
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
    backgroundColor: '#1e293b',
    color: '#f8fafc',
    fontFamily: 'Inter, system-ui, sans-serif',
  },
  header: {
    borderBottom: '1px solid #334155',
    paddingBottom: '16px',
    marginBottom: '24px',
  },
  subtitle: {
    color: '#94a3b8',
    margin: '4px 0 0 0',
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
    gap: '20px',
  },
  card: {
    backgroundColor: '#0f172a',
    padding: '20px',
    borderRadius: '12px',
    border: '1px solid #334155',
  },
  cardFull: {
    gridColumn: '1 / span 2',
    backgroundColor: '#0f172a',
    padding: '20px',
    borderRadius: '12px',
    border: '1px solid #334155',
  },
  cardText: {
    fontSize: '14px',
    color: '#94a3b8',
    lineHeight: '1.5',
    marginBottom: '16px',
  },
  formGroup: {
    marginBottom: '12px',
  },
  label: {
    display: 'block',
    fontSize: '13px',
    fontWeight: '600',
    color: '#94a3b8',
    marginBottom: '6px',
  },
  input: {
    width: '100%',
    padding: '10px 12px',
    borderRadius: '8px',
    border: '1px solid #334155',
    backgroundColor: '#1e293b',
    color: '#f8fafc',
    fontSize: '14px',
    outline: 'none',
    boxSizing: 'border-box',
  },
  configOutput: {
    marginTop: '16px',
    padding: '12px',
    backgroundColor: '#1e293b',
    borderRadius: '8px',
    fontSize: '12px',
    color: '#94a3b8',
  },
  keyRow: {
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '16px',
    fontSize: '14px',
  },
  highlight: {
    color: '#38bdf8',
  },
  primaryButton: {
    width: '100%',
    padding: '12px',
    backgroundColor: '#0284c7',
    border: 'none',
    borderRadius: '8px',
    color: '#fff',
    fontWeight: 'bold',
    cursor: 'pointer',
  },
  secondaryButton: {
    padding: '12px 24px',
    backgroundColor: '#4f46e5',
    border: 'none',
    borderRadius: '8px',
    color: '#fff',
    fontWeight: 'bold',
    cursor: 'pointer',
  },
  keyList: {
    marginTop: '20px',
    borderTop: '1px solid #334155',
    paddingTop: '16px',
  },
  keyItem: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: '8px 12px',
    backgroundColor: '#1e293b',
    borderRadius: '6px',
    marginBottom: '6px',
    fontSize: '13px',
  },
};
