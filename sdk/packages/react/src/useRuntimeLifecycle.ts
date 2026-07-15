import { useContext } from 'react';
import { RuntimeLifecycleContext } from './BootstrapProvider.js';
import type { RuntimeLifecycleState } from './BootstrapProvider.js';

export function useRuntimeLifecycle(): RuntimeLifecycleState {
  return useContext(RuntimeLifecycleContext);
}
