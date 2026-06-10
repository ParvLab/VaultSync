import type { RecordFields } from './types.js';

export type SubscriptionEvent = 'change' | 'error';
export type SubscriptionListener = (recordId: string, fields: RecordFields) => void;

export class Subscription {
  private listeners: Map<SubscriptionEvent, Set<SubscriptionListener>> = new Map();
  private unsubscribeFn?: () => void;

  constructor(unsubscribeFn: () => void) {
    this.unsubscribeFn = unsubscribeFn;
  }

  on(event: SubscriptionEvent, listener: SubscriptionListener): this {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener);
    return this;
  }

  off(event: SubscriptionEvent, listener: SubscriptionListener): this {
    const set = this.listeners.get(event);
    if (set) {
      set.delete(listener);
    }
    return this;
  }

  emit(event: SubscriptionEvent, recordId: string, fields: RecordFields): void {
    const set = this.listeners.get(event);
    if (set) {
      for (const listener of set) {
        try {
          listener(recordId, fields);
        } catch (e) {
          console.error('Error in subscription listener:', e);
        }
      }
    }
  }

  unsubscribe(): void {
    if (this.unsubscribeFn) {
      this.unsubscribeFn();
      this.unsubscribeFn = undefined;
    }
    this.listeners.clear();
  }
}
