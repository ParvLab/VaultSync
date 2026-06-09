use std::sync::{Arc, RwLock};
use crate::error::DriftError;
use crate::sync::state::SyncState;

#[repr(C)]
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

#[repr(C)]
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

const HEADER_SIZE: usize = 128; // Padded SharedMemoryHeader size

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

#[cfg(not(target_arch = "wasm32"))]
pub struct SharedMemoryInner {
    mmap: MmapMut,
    _file: std::fs::File,
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
    namespace: String,
}

impl SharedMemory {
    pub fn create_in_path(namespace: &str, path: std::path::PathBuf, size: usize) -> Result<Self, DriftError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .map_err(|e| DriftError::Storage(format!("failed to open mmap file: {e}")))?;
            
            file.set_len(size as u64)
                .map_err(|e| DriftError::Storage(format!("failed to set mmap file len: {e}")))?;

            let mmap = unsafe { MmapMut::map_mut(&file) }
                .map_err(|e| DriftError::Storage(format!("failed to mmap file: {e}")))?;

            let mut inner = SharedMemoryInner {
                mmap,
                _file: file,
                file_path: path,
                capacity: size - HEADER_SIZE,
            };

            // Initialize Header
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
            let mut buffer = vec![0; size];
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
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &header as *const SharedMemoryHeader as *const u8,
                    buffer.as_mut_ptr(),
                    std::mem::size_of::<SharedMemoryHeader>(),
                );
            }
            Ok(Self {
                inner: Arc::new(RwLock::new(SharedMemoryInner {
                    buffer,
                    capacity: size - HEADER_SIZE,
                })),
                namespace: namespace.to_string(),
            })
        }
    }

    pub fn create_with_namespace(namespace: &str, size: usize) -> Result<Self, DriftError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!("drift_shm_{}.bin", namespace));
            Self::create_in_path(namespace, path, size)
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::create_in_path(namespace, std::path::PathBuf::new(), size)
        }
    }

    pub fn create(size: usize) -> Result<Self, DriftError> {
        Self::create_with_namespace("default", size)
    }

    pub fn open(namespace: &str) -> Result<Self, DriftError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!("drift_shm_{}.bin", namespace));
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| DriftError::Storage(format!("failed to open existing mmap file: {e}")))?;
            
            let metadata = file.metadata()
                .map_err(|e| DriftError::Storage(format!("failed to read file metadata: {e}")))?;
            let size = metadata.len() as usize;

            let mmap = unsafe { MmapMut::map_mut(&file) }
                .map_err(|e| DriftError::Storage(format!("failed to mmap file: {e}")))?;

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

    pub fn write_entry(&self, id: &str, yrs_update: &[u8]) -> Result<(), DriftError> {
        let mut inner = self.inner.write().map_err(|e| DriftError::Storage(e.to_string()))?;
        
        let entry_header_size = std::mem::size_of::<RingBufferEntryHeader>();
        let needed_size = entry_header_size + yrs_update.len();
        
        if needed_size > inner.capacity {
            return Err(DriftError::Storage("mutation too large for ring buffer".to_string()));
        }

        #[cfg(not(target_arch = "wasm32"))]
        let slice = &mut inner.mmap[..];
        #[cfg(target_arch = "wasm32")]
        let slice = &mut inner.buffer[..];

        let mut header = read_header(slice);
        
        let mut head = header.ring_buffer_head as usize;
        let mut tail = header.ring_buffer_tail as usize;
        let capacity = header.ring_buffer_capacity as usize;

        // Check wrap around
        if tail + needed_size > capacity {
            // Write wrap marker at tail
            if tail + entry_header_size <= capacity {
                let marker = RingBufferEntryHeader {
                    id: [0; 64],
                    yrs_update_size: 0xFFFFFFFF,
                    timestamp: 0,
                };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &marker as *const RingBufferEntryHeader as *const u8,
                        slice[HEADER_SIZE + tail..].as_mut_ptr(),
                        entry_header_size,
                    );
                }
            }
            tail = 0;
        }

        // Free space by advancing head if it is overlapped by our new write
        let new_tail = tail + needed_size;
        
        // Loop to advance head until it is no longer within the [tail, new_tail) range
        // Note: we also check if head is at the end (wrapped)
        while head_is_between(head, tail, new_tail, capacity) {
            // Read head entry size to skip it
            if head + entry_header_size > capacity {
                head = 0;
                continue;
            }
            
            let mut entry_h = RingBufferEntryHeader {
                id: [0; 64],
                yrs_update_size: 0,
                timestamp: 0,
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    slice[HEADER_SIZE + head..].as_ptr(),
                    &mut entry_h as *mut RingBufferEntryHeader as *mut u8,
                    entry_header_size,
                );
            }
            
            if entry_h.yrs_update_size == 0xFFFFFFFF {
                head = 0;
            } else {
                head += entry_header_size + entry_h.yrs_update_size as usize;
            }
        }

        // Write new entry
        let mut id_bytes = [0u8; 64];
        let id_len = id.len().min(64);
        id_bytes[..id_len].copy_from_slice(&id.as_bytes()[..id_len]);

        let entry_h = RingBufferEntryHeader {
            id: id_bytes,
            yrs_update_size: yrs_update.len() as u32,
            timestamp: crate::time_utils::system_time_now_ms(),
        };

        // Copy header
        unsafe {
            std::ptr::copy_nonoverlapping(
                &entry_h as *const RingBufferEntryHeader as *const u8,
                slice[HEADER_SIZE + tail..].as_mut_ptr(),
                entry_header_size,
            );
        }

        // Copy bytes
        slice[HEADER_SIZE + tail + entry_header_size..HEADER_SIZE + tail + needed_size]
            .copy_from_slice(yrs_update);

        header.ring_buffer_head = head as u32;
        header.ring_buffer_tail = new_tail as u32;
        write_header(slice, &header);

        Ok(())
    }

    pub fn read_uncommitted(&self) -> Result<Vec<RingBufferEntry>, DriftError> {
        let inner = self.inner.read().map_err(|e| DriftError::Storage(e.to_string()))?;

        #[cfg(not(target_arch = "wasm32"))]
        let slice = &inner.mmap[..];
        #[cfg(target_arch = "wasm32")]
        let slice = &inner.buffer[..];

        let header = read_header(slice);
        let mut head = header.ring_buffer_head as usize;
        let tail = header.ring_buffer_tail as usize;
        let capacity = header.ring_buffer_capacity as usize;

        let mut entries = Vec::new();
        let entry_header_size = std::mem::size_of::<RingBufferEntryHeader>();

        if head == tail {
            return Ok(entries);
        }

        while head != tail {
            if head + entry_header_size > capacity {
                head = 0;
                if head == tail { break; }
            }

            let mut entry_h = RingBufferEntryHeader {
                id: [0; 64],
                yrs_update_size: 0,
                timestamp: 0,
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    slice[HEADER_SIZE + head..].as_ptr(),
                    &mut entry_h as *mut RingBufferEntryHeader as *mut u8,
                    entry_header_size,
                );
            }

            if entry_h.yrs_update_size == 0xFFFFFFFF {
                head = 0;
                continue;
            }

            let next_head = head + entry_header_size + entry_h.yrs_update_size as usize;
            if next_head > capacity {
                // Malformed or wrapped
                break;
            }

            let id_str = String::from_utf8_lossy(&entry_h.id)
                .trim_end_matches('\0')
                .to_string();

            let yrs_update = slice[HEADER_SIZE + head + entry_header_size..HEADER_SIZE + next_head].to_vec();

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
    pub fn write(&self, data: &[u8]) -> Result<(), DriftError> {
        let mut inner = self.inner.write().map_err(|e| DriftError::Storage(e.to_string()))?;
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

    pub fn read(&self) -> Result<Vec<u8>, DriftError> {
        let inner = self.inner.read().map_err(|e| DriftError::Storage(e.to_string()))?;
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

fn head_is_between(head: usize, start: usize, end: usize, _capacity: usize) -> bool {
    if start <= end {
        head >= start && head < end
    } else {
        head >= start || head < end
    }
}

fn read_header(mmap: &[u8]) -> SharedMemoryHeader {
    let mut header = SharedMemoryHeader {
        leader_id: [0; 64],
        leader_timestamp: 0,
        generation: 0,
        ring_buffer_head: 0,
        ring_buffer_tail: 0,
        ring_buffer_capacity: 0,
        snapshot_offset: 0,
        snapshot_size: 0,
    };
    unsafe {
        std::ptr::copy_nonoverlapping(
            mmap.as_ptr(),
            &mut header as *mut SharedMemoryHeader as *mut u8,
            std::mem::size_of::<SharedMemoryHeader>(),
        );
    }
    header
}

fn write_header(mmap: &mut [u8], header: &SharedMemoryHeader) {
    unsafe {
        std::ptr::copy_nonoverlapping(
            header as *const SharedMemoryHeader as *const u8,
            mmap.as_mut_ptr(),
            std::mem::size_of::<SharedMemoryHeader>(),
        );
    }
}

pub fn encode_sync_state(state: &SyncState) -> Result<Vec<u8>, DriftError> {
    bincode::serialize(state).map_err(|e| DriftError::Storage(format!("serialization failed: {e}")))
}

pub fn decode_sync_state(bytes: &[u8]) -> Option<SyncState> {
    bincode::deserialize(bytes).ok()
}
