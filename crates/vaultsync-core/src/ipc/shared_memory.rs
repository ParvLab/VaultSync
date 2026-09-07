use crate::error::VaultSyncError;
use crate::sync::state::SyncState;
use std::sync::{Arc, RwLock};

// ── Wire-format sizes (packed, no struct padding) ───────────────────────────
//
// SharedMemoryHeader wire layout (fits in HEADER_SIZE = 128 bytes):
//   [  0.. 64]  leader_id      (64 bytes)
//   [ 64.. 72]  leader_timestamp  (u64 LE, 8 bytes)
//   [ 72.. 80]  generation        (u64 LE, 8 bytes)
//   [ 80.. 84]  ring_buffer_head  (u32 LE, 4 bytes)
//   [ 84.. 88]  ring_buffer_tail  (u32 LE, 4 bytes)
//   [ 88.. 92]  ring_buffer_capacity (u32 LE, 4 bytes)
//   [ 92..128]  reserved / padding
//
// RingBufferEntryHeader wire layout (ENTRY_HEADER_WIRE_SIZE = 76 bytes):
//   [  0.. 64]  id             (64 bytes)
//   [ 64.. 68]  yrs_update_size (u32 LE, 4 bytes)
//   [ 68.. 76]  timestamp       (u64 LE, 8 bytes)
//
// Previously, copy_nonoverlapping used sizeof(RingBufferEntryHeader) = 80
// (because repr(C) inserts 4-byte padding between u32 and u64 for alignment).
// That caused the head-advance loop to mis-decode yrs_update_size,
// resulting in wild head offsets and WASM heap corruption (dlmalloc panic).

const HEADER_SIZE: usize = 128;
const ENTRY_HEADER_WIRE_SIZE: usize = 76; // 64 + 4 + 8, packed

// These structs are kept for semantic clarity but are never cast to raw bytes.
#[derive(Debug, Clone, Copy)]
pub struct SharedMemoryHeader {
    pub leader_id: [u8; 64],
    pub leader_timestamp: u64,
    pub generation: u64,
    pub ring_buffer_head: u32,
    pub ring_buffer_tail: u32,
    pub ring_buffer_capacity: u32,
    pub snapshot_offset: u64,
    pub snapshot_size: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct RingBufferEntryHeader {
    pub id: [u8; 64],
    pub yrs_update_size: u32,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct RingBufferEntry {
    pub id: String,
    pub yrs_update: Vec<u8>,
    pub timestamp: u64,
}

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

#[cfg(not(target_arch = "wasm32"))]
pub struct SharedMemoryInner {
    mmap: MmapMut,
    _file: std::fs::File,
    #[allow(dead_code)]
    file_path: std::path::PathBuf,
    capacity: usize,
}

#[cfg(target_arch = "wasm32")]
pub struct SharedMemoryInner {
    buffer: Vec<u8>,
    capacity: usize,
}

#[derive(Clone)]
pub struct SharedMemory {
    inner: Arc<RwLock<SharedMemoryInner>>,
    #[allow(dead_code)]
    namespace: String,
}

// ── Explicit wire-format encode / decode (no unsafe pointer casts) ──────────

fn encode_header(h: &SharedMemoryHeader) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..64].copy_from_slice(&h.leader_id);
    buf[64..72].copy_from_slice(&h.leader_timestamp.to_le_bytes());
    buf[72..80].copy_from_slice(&h.generation.to_le_bytes());
    buf[80..84].copy_from_slice(&h.ring_buffer_head.to_le_bytes());
    buf[84..88].copy_from_slice(&h.ring_buffer_tail.to_le_bytes());
    buf[88..92].copy_from_slice(&h.ring_buffer_capacity.to_le_bytes());
    buf[92..100].copy_from_slice(&h.snapshot_offset.to_le_bytes());
    buf[100..108].copy_from_slice(&h.snapshot_size.to_le_bytes());
    // [108..128] left as zero padding
    buf
}

fn decode_header(buf: &[u8]) -> SharedMemoryHeader {
    if buf.len() < HEADER_SIZE {
        return SharedMemoryHeader {
            leader_id: [0; 64],
            leader_timestamp: 0,
            generation: 0,
            ring_buffer_head: 0,
            ring_buffer_tail: 0,
            ring_buffer_capacity: 0,
            snapshot_offset: 0,
            snapshot_size: 0,
        };
    }
    let mut leader_id = [0u8; 64];
    leader_id.copy_from_slice(&buf[0..64]);
    SharedMemoryHeader {
        leader_id,
        leader_timestamp: u64::from_le_bytes(buf[64..72].try_into().unwrap_or([0; 8])),
        generation: u64::from_le_bytes(buf[72..80].try_into().unwrap_or([0; 8])),
        ring_buffer_head: u32::from_le_bytes(buf[80..84].try_into().unwrap_or([0; 4])),
        ring_buffer_tail: u32::from_le_bytes(buf[84..88].try_into().unwrap_or([0; 4])),
        ring_buffer_capacity: u32::from_le_bytes(buf[88..92].try_into().unwrap_or([0; 4])),
        snapshot_offset: u64::from_le_bytes(buf[92..100].try_into().unwrap_or([0; 8])),
        snapshot_size: u64::from_le_bytes(buf[100..108].try_into().unwrap_or([0; 8])),
    }
}

fn encode_entry_header(h: &RingBufferEntryHeader) -> [u8; ENTRY_HEADER_WIRE_SIZE] {
    let mut buf = [0u8; ENTRY_HEADER_WIRE_SIZE];
    buf[0..64].copy_from_slice(&h.id);
    buf[64..68].copy_from_slice(&h.yrs_update_size.to_le_bytes());
    buf[68..76].copy_from_slice(&h.timestamp.to_le_bytes());
    buf
}

fn decode_entry_header(buf: &[u8]) -> Option<RingBufferEntryHeader> {
    if buf.len() < ENTRY_HEADER_WIRE_SIZE {
        return None;
    }
    let mut id = [0u8; 64];
    id.copy_from_slice(&buf[0..64]);
    Some(RingBufferEntryHeader {
        id,
        yrs_update_size: u32::from_le_bytes(buf[64..68].try_into().ok()?),
        timestamp: u64::from_le_bytes(buf[68..76].try_into().ok()?),
    })
}

// ── read_header / write_header using explicit encoding ────────────────────────

fn read_header(mmap: &[u8]) -> SharedMemoryHeader {
    decode_header(mmap)
}

fn write_header(mmap: &mut [u8], header: &SharedMemoryHeader) {
    if mmap.len() >= HEADER_SIZE {
        let encoded = encode_header(header);
        mmap[0..HEADER_SIZE].copy_from_slice(&encoded);
    }
}

// ── SharedMemory implementation ───────────────────────────────────────────────

impl SharedMemory {
    pub fn create_in_path(
        namespace: &str,
        path: std::path::PathBuf,
        size: usize,
    ) -> Result<Self, VaultSyncError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .map_err(|e| VaultSyncError::Storage(format!("failed to open mmap file: {e}")))?;

            file.set_len(size as u64).map_err(|e| {
                VaultSyncError::Storage(format!("failed to set mmap file len: {e}"))
            })?;

            let mmap = unsafe { MmapMut::map_mut(&file) }
                .map_err(|e| VaultSyncError::Storage(format!("failed to mmap file: {e}")))?;

            let mut inner = SharedMemoryInner {
                mmap,
                _file: file,
                file_path: path,
                capacity: size - HEADER_SIZE,
            };

            let header = SharedMemoryHeader {
                leader_id: [0; 64],
                leader_timestamp: 0,
                generation: 0,
                ring_buffer_head: 0,
                ring_buffer_tail: 0,
                ring_buffer_capacity: (size - HEADER_SIZE) as u32,
                snapshot_offset: 0,
                snapshot_size: 0,
            };
            write_header(&mut inner.mmap, &header);

            Ok(Self {
                inner: Arc::new(RwLock::new(inner)),
                namespace: namespace.to_string(),
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = path;
            let mut buffer = vec![0u8; size];
            let header = SharedMemoryHeader {
                leader_id: [0; 64],
                leader_timestamp: 0,
                generation: 0,
                ring_buffer_head: 0,
                ring_buffer_tail: 0,
                ring_buffer_capacity: (size - HEADER_SIZE) as u32,
                snapshot_offset: 0,
                snapshot_size: 0,
            };
            // Use explicit encoding — no unsafe struct-cast
            write_header(&mut buffer, &header);
            Ok(Self {
                inner: Arc::new(RwLock::new(SharedMemoryInner {
                    buffer,
                    capacity: size - HEADER_SIZE,
                })),
                namespace: namespace.to_string(),
            })
        }
    }

    pub fn create_with_namespace(namespace: &str, size: usize) -> Result<Self, VaultSyncError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!("vaultsync_shm_{}.bin", namespace));
            Self::create_in_path(namespace, path, size)
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::create_in_path(namespace, std::path::PathBuf::new(), size)
        }
    }

    pub fn create(size: usize) -> Result<Self, VaultSyncError> {
        Self::create_with_namespace("default", size)
    }

    pub fn open(namespace: &str) -> Result<Self, VaultSyncError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!("vaultsync_shm_{}.bin", namespace));
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| {
                    VaultSyncError::Storage(format!("failed to open existing mmap file: {e}"))
                })?;

            let metadata = file.metadata().map_err(|e| {
                VaultSyncError::Storage(format!("failed to read file metadata: {e}"))
            })?;
            let size = metadata.len() as usize;

            let mmap = unsafe { MmapMut::map_mut(&file) }
                .map_err(|e| VaultSyncError::Storage(format!("failed to mmap file: {e}")))?;

            let inner = SharedMemoryInner {
                mmap,
                _file: file,
                file_path: path,
                capacity: size - HEADER_SIZE,
            };

            Ok(Self {
                inner: Arc::new(RwLock::new(inner)),
                namespace: namespace.to_string(),
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::create_with_namespace(namespace, 64 * 1024)
        }
    }

    pub fn write_entry(&self, id: &str, yrs_update: &[u8]) -> Result<(), VaultSyncError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;

        // Use the wire-format size (76 bytes), NOT sizeof() which includes padding
        let needed_size = ENTRY_HEADER_WIRE_SIZE + yrs_update.len();

        if needed_size > inner.capacity {
            return Err(VaultSyncError::Storage(
                "mutation too large for ring buffer".to_string(),
            ));
        }

        #[cfg(not(target_arch = "wasm32"))]
        let slice = &mut inner.mmap[..];
        #[cfg(target_arch = "wasm32")]
        let slice = &mut inner.buffer[..];

        let mut header = read_header(slice);

        let mut head = header.ring_buffer_head as usize;
        let mut tail = header.ring_buffer_tail as usize;
        let capacity = header.ring_buffer_capacity as usize;

        // Validate header fields to prevent OOB access from corrupted state
        let max_capacity = slice.len().saturating_sub(HEADER_SIZE);
        if capacity > max_capacity {
            return Err(VaultSyncError::Storage(
                "invalid ring buffer capacity in header".to_string(),
            ));
        }
        if head > capacity || tail > capacity {
            head = 0;
            tail = 0;
        }

        // Decide if we need to wrap tail to the start
        let wrap_around = tail.checked_add(needed_size).map_or(true, |v| v > capacity);

        if wrap_around {
            // Write a wrap sentinel at the current tail position if there's room
            if tail.checked_add(ENTRY_HEADER_WIRE_SIZE).map_or(false, |v| v <= capacity) {
                let sentinel = RingBufferEntryHeader {
                    id: [0; 64],
                    yrs_update_size: 0xFFFF_FFFF,
                    timestamp: 0,
                };
                let wire = encode_entry_header(&sentinel);
                let offset = HEADER_SIZE + tail;
                // bounds guaranteed by the check above
                slice[offset..offset + ENTRY_HEADER_WIRE_SIZE].copy_from_slice(&wire);
            }
            tail = 0;
        }

        // new_tail is where the entry will end
        let new_tail = tail.checked_add(needed_size).filter(|&v| v <= capacity).ok_or_else(|| {
            VaultSyncError::Storage("calculated tail out of bounds".to_string())
        })?;

        // Advance head past any entries that our write would overwrite
        let mut loop_count = 0u32;
        while head_is_between(head, tail, new_tail, capacity) {
            loop_count += 1;
            if loop_count > 10_000 {
                head = 0;
                tail = 0;
                break;
            }

            // Make sure there's room to read the entry header at head
            let head_end = head.checked_add(ENTRY_HEADER_WIRE_SIZE).unwrap_or(0);
            if head_end > capacity {
                head = 0;
                continue;
            }

            let head_offset = HEADER_SIZE + head;
            let head_slice_end = head_offset + ENTRY_HEADER_WIRE_SIZE;
            if head_slice_end > slice.len() {
                head = 0;
                continue;
            }

            let entry_h = match decode_entry_header(&slice[head_offset..head_slice_end]) {
                Some(h) => h,
                None => {
                    head = 0;
                    continue;
                }
            };

            if entry_h.yrs_update_size == 0xFFFF_FFFF {
                // Wrap sentinel — jump head back to start
                head = 0;
            } else {
                let payload = entry_h.yrs_update_size as usize;
                let skip = ENTRY_HEADER_WIRE_SIZE.checked_add(payload).unwrap_or(0);
                let next = head.checked_add(skip).unwrap_or(0);
                // If next would go past capacity, reset to 0
                head = if next <= capacity { next } else { 0 };
            }
        }

        // Write the new entry at tail
        let mut id_bytes = [0u8; 64];
        let id_src = id.as_bytes();
        let copy_len = id_src.len().min(64);
        id_bytes[..copy_len].copy_from_slice(&id_src[..copy_len]);

        let entry_h = RingBufferEntryHeader {
            id: id_bytes,
            yrs_update_size: yrs_update.len() as u32,
            timestamp: crate::time_utils::system_time_now_ms(),
        };
        let wire = encode_entry_header(&entry_h);

        let tail_offset = HEADER_SIZE + tail;
        let payload_offset = tail_offset + ENTRY_HEADER_WIRE_SIZE;
        let payload_end = payload_offset + yrs_update.len();

        if payload_end > slice.len() {
            return Err(VaultSyncError::Storage("write out of bounds".to_string()));
        }

        slice[tail_offset..tail_offset + ENTRY_HEADER_WIRE_SIZE].copy_from_slice(&wire);
        slice[payload_offset..payload_end].copy_from_slice(yrs_update);

        header.ring_buffer_head = head as u32;
        header.ring_buffer_tail = new_tail as u32;
        write_header(slice, &header);

        Ok(())
    }

    pub fn read_uncommitted(&self) -> Result<Vec<RingBufferEntry>, VaultSyncError> {
        let inner = self
            .inner
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;

        #[cfg(not(target_arch = "wasm32"))]
        let slice = &inner.mmap[..];
        #[cfg(target_arch = "wasm32")]
        let slice = &inner.buffer[..];

        let header = read_header(slice);
        let mut head = header.ring_buffer_head as usize;
        let tail = header.ring_buffer_tail as usize;
        let capacity = header.ring_buffer_capacity as usize;

        let max_capacity = slice.len().saturating_sub(HEADER_SIZE);
        if capacity > max_capacity {
            return Err(VaultSyncError::Storage(
                "invalid ring buffer capacity in header".to_string(),
            ));
        }
        if head > capacity || tail > capacity {
            return Ok(Vec::new());
        }
        if head == tail {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        let mut loop_count = 0u32;

        while head != tail {
            loop_count += 1;
            if loop_count > 10_000 {
                break;
            }

            // Bounds-check: can we read an entry header here?
            let head_end = head.checked_add(ENTRY_HEADER_WIRE_SIZE).unwrap_or(0);
            if head_end > capacity {
                // Wrap: jump to start
                head = 0;
                if head == tail {
                    break;
                }
                continue;
            }

            let head_offset = HEADER_SIZE + head;
            let head_slice_end = head_offset + ENTRY_HEADER_WIRE_SIZE;
            if head_slice_end > slice.len() {
                head = 0;
                if head == tail {
                    break;
                }
                continue;
            }

            let entry_h = match decode_entry_header(&slice[head_offset..head_slice_end]) {
                Some(h) => h,
                None => break,
            };

            if entry_h.yrs_update_size == 0xFFFF_FFFF {
                // Wrap sentinel
                head = 0;
                if head == tail {
                    break;
                }
                continue;
            }

            let payload_len = entry_h.yrs_update_size as usize;
            let next_head = head
                .checked_add(ENTRY_HEADER_WIRE_SIZE)
                .and_then(|v| v.checked_add(payload_len))
                .filter(|&v| v <= capacity);

            let next_head = match next_head {
                Some(v) => v,
                None => break, // malformed entry, stop
            };

            let payload_start = head_slice_end;
            let payload_end = payload_start + payload_len;
            if payload_end > slice.len() {
                break;
            }

            let id_str = String::from_utf8_lossy(&entry_h.id)
                .trim_end_matches('\0')
                .to_string();
            let yrs_update = slice[payload_start..payload_end].to_vec();

            entries.push(RingBufferEntry {
                id: id_str,
                yrs_update,
                timestamp: entry_h.timestamp,
            });

            head = next_head;
        }

        Ok(entries)
    }

    // Raw read/write for legacy tests
    pub fn write(&self, data: &[u8]) -> Result<(), VaultSyncError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            let len = data.len().min(inner.mmap.len());
            inner.mmap[..len].copy_from_slice(&data[..len]);
            if len < inner.mmap.len() {
                inner.mmap[len..].fill(0);
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            let len = data.len().min(inner.buffer.len());
            inner.buffer[..len].copy_from_slice(&data[..len]);
            if len < inner.buffer.len() {
                inner.buffer[len..].fill(0);
            }
        }
        Ok(())
    }

    pub fn read(&self) -> Result<Vec<u8>, VaultSyncError> {
        let inner = self
            .inner
            .read()
            .map_err(|e| VaultSyncError::Storage(e.to_string()))?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            Ok(inner.mmap.to_vec())
        }
        #[cfg(target_arch = "wasm32")]
        {
            Ok(inner.buffer.clone())
        }
    }
}

// ── head_is_between helper ────────────────────────────────────────────────────

fn head_is_between(head: usize, start: usize, end: usize, _capacity: usize) -> bool {
    if start <= end {
        head >= start && head < end
    } else {
        head >= start || head < end
    }
}

// ── SyncState serialization ───────────────────────────────────────────────────

pub fn encode_sync_state(state: &SyncState) -> Result<Vec<u8>, VaultSyncError> {
    bincode::serialize(state)
        .map_err(|e| VaultSyncError::Storage(format!("serialization failed: {e}")))
}

pub fn decode_sync_state(bytes: &[u8]) -> Option<SyncState> {
    bincode::deserialize(bytes).ok()
}
