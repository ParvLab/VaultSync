import { useState, useEffect } from 'react';
import { useVaultSyncClient } from '@vaultsync/react';

export function EncryptionBadge() {
  const client = useVaultSyncClient();
  const [activeVersion, setActiveVersion] = useState<number | null>(null);
  const [isRotating, setIsRotating] = useState(false);

  const fetchKeyInfo = () => {
    try {
      const version = client.keys.activeVersion();
      setActiveVersion(version);
    } catch (e) {
      // Keys might not be fully initialized or E2EE isn't enabled with a key
      setActiveVersion(null);
    }
  };

  useEffect(() => {
    fetchKeyInfo();
    const interval = setInterval(fetchKeyInfo, 2000);
    return () => clearInterval(interval);
  }, [client]);

  const handleRotateKeys = async () => {
    if (isRotating) return;
    setIsRotating(true);
    try {
      await client.keys.rotate();
      fetchKeyInfo();
    } catch (err) {
      console.error('Failed to rotate encryption keys:', err);
    } finally {
      setIsRotating(false);
    }
  };

  return (
    <div 
      className="indicator-item" 
      style={{ 
        cursor: 'pointer',
        borderColor: 'rgba(99, 102, 241, 0.2)',
        background: 'rgba(99, 102, 241, 0.05)'
      }}
      onClick={handleRotateKeys}
      title="End-to-End Encrypted. Click to rotate keys."
      data-testid="e2ee-status"
    >
      <span style={{ color: '#a5b4fc' }}>🔒 E2EE Active</span>
      <span 
        style={{ 
          fontSize: '0.75rem', 
          background: 'rgba(255, 255, 255, 0.08)', 
          padding: '0.1rem 0.35rem', 
          borderRadius: '4px',
          fontFamily: 'monospace'
        }}
        data-testid="key-version"
      >
        {isRotating ? 'Rotating...' : `v${activeVersion ?? 1}`}
      </span>
    </div>
  );
}
