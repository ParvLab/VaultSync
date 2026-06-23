use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

/// Hybrid Logical Clock timestamp.
///
/// Combines wall clock time (ms since epoch) with a logical counter
/// to provide causally-ordered timestamps even when system clocks drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HlcTimestamp {
    pub wall: u64,
    #[serde(default)]
    pub logical: u32,
}

impl Default for HlcTimestamp {
    fn default() -> Self {
        Self {
            wall: 0,
            logical: 0,
        }
    }
}

impl HlcTimestamp {
    pub fn new(wall: u64, logical: u32) -> Self {
        Self { wall, logical }
    }
}

/// Hybrid Logical Clock for generating monotonic timestamps.
///
/// Thread-safe via atomics. Call `now()` to get the current HLC time.
pub struct HybridLogicalClock {
    wall: AtomicU64,
    logical: AtomicU32,
    wrapped: AtomicBool,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        let wall = crate::time_utils::system_time_now_ms();
        Self {
            wall: AtomicU64::new(wall),
            logical: AtomicU32::new(0),
            wrapped: AtomicBool::new(false),
        }
    }

    /// Generate a new HLC timestamp based on the current system clock.
    ///
    /// If system time > stored wall, jumps forward and resets logical.
    /// If system time == stored wall, increments logical.
    /// If system time < stored wall (clock went backwards), increments logical.
    pub fn now(&self) -> HlcTimestamp {
        let sys = crate::time_utils::system_time_now_ms();
        loop {
            let w = self.wall.load(Ordering::SeqCst);
            let l = self.logical.load(Ordering::SeqCst);

            let (new_wall, new_logical) = if sys > w {
                (sys, 0)
            } else if sys == w {
                (w, l.wrapping_add(1))
            } else {
                (w, l.wrapping_add(1))
            };

            if self
                .wall
                .compare_exchange(w, new_wall, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.logical.store(new_logical, Ordering::SeqCst);
                if new_logical == 0 && l > 0 {
                    self.wrapped.store(true, Ordering::SeqCst);
                }
                return HlcTimestamp {
                    wall: new_wall,
                    logical: new_logical,
                };
            }
        }
    }

    /// Update the clock with a received HLC timestamp (from a remote peer).
    /// Ensures local clock >= remote timestamp for causal ordering.
    pub fn update_with_received(&self, received: &HlcTimestamp) {
        loop {
            let w = self.wall.load(Ordering::SeqCst);
            let l = self.logical.load(Ordering::SeqCst);

            let new_wall = w.max(received.wall).max(crate::time_utils::system_time_now_ms());
            let new_logical = if new_wall == w {
                l.max(received.logical).wrapping_add(1)
            } else if new_wall == received.wall {
                received.logical.wrapping_add(1)
            } else {
                0
            };

            if self
                .wall
                .compare_exchange(w, new_wall, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.logical.store(new_logical, Ordering::SeqCst);
                return;
            }
        }
    }

    /// Returns true if the logical counter wrapped (overflowed to 0) since last call to `clear_logical_wrap()`.
    pub fn did_logical_wrap(&self) -> bool {
        self.wrapped.load(Ordering::SeqCst)
    }

    /// Reset the logical wrap flag after recording it.
    pub fn clear_logical_wrap(&self) {
        self.wrapped.store(false, Ordering::SeqCst);
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}
