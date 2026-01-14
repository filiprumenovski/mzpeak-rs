# AsyncMzPeakWriter Technical Specification

**Author:** Principal Engineer Review  
**Date:** January 14, 2026  
**Status:** Implementation Ready

---

## 1. Problem Statement

The current `MzPeakWriter` performs CPU-intensive compression (ZSTD) and blocking disk I/O on the caller's thread, stalling the data preparation ("gather") phase. For high-throughput pipelines processing millions of peaks per second, this creates a bottleneck where the producer waits for I/O instead of preparing the next batch.

## 2. Solution Overview

Implement an **asynchronous pipeline architecture** that offloads compression and I/O to a dedicated background thread, connected via a bounded channel for backpressure control.

```
┌─────────────┐     bounded channel      ┌─────────────────┐
│  Producer   │ ──OwnedColumnarBatch──▶  │ Background      │
│  (caller)   │      (pointer move)      │ Writer Thread   │
│             │ ◀────error slot──────    │ (compression+IO)│
└─────────────┘   Arc<Mutex<Option>>     └─────────────────┘
```

## 3. Zero-Copy Guarantee

**Critical invariant:** No data bytes are copied in the async path.

### Data Flow Analysis

1. Producer creates `OwnedColumnarBatch` with `Vec<T>` fields
2. `write_owned_batch()` sends batch through channel → **pointer move, not copy**
3. Background thread receives batch
4. `MzPeakWriter::write_owned_batch()` consumes batch:
   - `ScalarBuffer::from(Vec<T>)` transfers heap ownership to Arrow → **zero-copy**
   - Arrow passes buffer to Parquet encoder
   - Parquet compresses and writes → **only compression touches bytes**

### Why This Works

- `OwnedColumnarBatch` contains only `Vec<T>` where `T` is a primitive (`f64`, `f32`, `i64`, `i16`, `i8`, `i32`)
- `Vec<T>` is `Send` when `T: Send` (all primitives are `Send`)
- Channel send moves the struct (3 words: ptr, len, cap per Vec) without touching heap data
- `ScalarBuffer::from(Vec<T>)` is documented as zero-copy ownership transfer

## 4. Error Handling Strategy

### Problem: `WriterError` is not `Send`

The error type contains `arrow::error::ArrowError` and `parquet::errors::ParquetError`, neither of which implement `Send`. This prevents returning `WriterError` directly from the background thread.

### Solution: Shared Error Slot + String Conversion

```rust
struct AsyncMzPeakWriter {
    sender: Option<Sender<OwnedColumnarBatch>>,
    handle: Option<JoinHandle<Result<WriterStats, String>>>,
    first_error: Arc<Mutex<Option<String>>>,
}
```

**Error propagation flow:**

1. Background thread encounters error → converts to `String` → stores in `first_error` slot → breaks loop → returns `Err(String)`
2. Next `write_owned_batch()` call checks `first_error` **before** sending → returns `WriterError::BackgroundWriterError` immediately
3. `finish()` joins thread → converts `String` to `WriterError::BackgroundWriterError`

**Why fail-fast matters (lossless contract):**

If we don't check `first_error` before sending, the caller could send 1000 batches after a disk-full error, believing they were written. This violates the lossless guarantee.

## 5. New Error Variants

Add to `WriterError` in `src/writer/error.rs`:

```rust
/// Error from the background writer thread
#[error("Background writer error: {0}")]
BackgroundWriterError(String),

/// Background writer thread panicked
#[error("Background writer thread panicked")]
ThreadPanicked,
```

## 6. Configuration Changes

### Compression Level

**File:** `src/writer/config.rs`

**Change:** `WriterConfig::default()` compression from `Zstd(9)` to `Zstd(3)`

**Rationale:**
- `Zstd(9)` is ~3-5x slower than `Zstd(3)` with diminishing compression gains
- `CompressionType::default()` already returns `Zstd(3)`
- `CompressionType::balanced()` returns `Zstd(3)`
- This aligns all defaults

### Buffer Capacity

**Add to `WriterConfig`:**

```rust
/// Channel buffer capacity for AsyncMzPeakWriter (number of batches)
/// Higher values increase memory usage but reduce producer stalls.
/// Default: 32 batches
pub async_buffer_capacity: usize,
```

**Default:** 32 batches

**Memory budget example:**
- 100K peaks/batch × 32 batches = 3.2M peaks in flight
- Per peak: 8 (mz) + 4 (intensity) + 8 (spectrum_id) + ... ≈ 50 bytes
- Total: ~160 MB max in-flight (acceptable for modern systems)

## 7. Struct Definition

**File:** `src/writer/async_writer.rs`

```rust
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::io::Write;

use crossbeam_channel::{bounded, Sender, SendError};

use crate::metadata::MzPeakMetadata;
use super::{MzPeakWriter, OwnedColumnarBatch, WriterConfig, WriterError, WriterStats};

/// Asynchronous wrapper for MzPeakWriter that offloads compression and I/O
/// to a dedicated background thread.
///
/// # Zero-Copy Guarantee
///
/// Batches are transferred through the channel via pointer move (not copy).
/// The underlying heap data is never duplicated—only ownership is transferred.
///
/// # Backpressure
///
/// Uses a bounded channel to prevent OOM if the producer is faster than the
/// writer. When the channel is full, `write_owned_batch()` blocks until space
/// is available.
///
/// # Error Handling
///
/// Errors in the background thread are detected on the next `write_owned_batch()`
/// call or on `finish()`. This ensures fail-fast behavior—the caller learns of
/// errors promptly rather than silently losing data.
pub struct AsyncMzPeakWriter {
    /// Channel sender (None after finish() is called)
    sender: Option<Sender<OwnedColumnarBatch>>,
    /// Background thread handle (None after finish() is called)  
    handle: Option<JoinHandle<Result<WriterStats, String>>>,
    /// First error encountered by background thread (for fail-fast detection)
    first_error: Arc<Mutex<Option<String>>>,
}
```

## 8. Constructor

```rust
impl AsyncMzPeakWriter {
    /// Create a new async writer that offloads work to a background thread.
    ///
    /// # Arguments
    ///
    /// * `writer` - The underlying Write implementation (e.g., File)
    /// * `metadata` - MzPeak metadata to embed in the Parquet file
    /// * `config` - Writer configuration including compression and buffer capacity
    ///
    /// # Panics
    ///
    /// Panics if the background thread fails to spawn (extremely rare).
    pub fn new<W>(
        writer: W,
        metadata: MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError>
    where
        W: Write + Send + Sync + 'static,
    {
        let buffer_capacity = config.async_buffer_capacity;
        let (sender, receiver) = bounded::<OwnedColumnarBatch>(buffer_capacity);
        let first_error = Arc::new(Mutex::new(None));
        let first_error_clone = Arc::clone(&first_error);

        // Spawn background writer thread
        let handle = thread::Builder::new()
            .name("mzpeak-writer".to_string())
            .spawn(move || {
                // Initialize writer inside thread (config is moved)
                let mut inner_writer = match MzPeakWriter::new(writer, &metadata, config) {
                    Ok(w) => w,
                    Err(e) => {
                        let err_str = e.to_string();
                        *first_error_clone.lock().unwrap() = Some(err_str.clone());
                        return Err(err_str);
                    }
                };

                // Process batches until channel disconnects
                for batch in receiver {
                    if let Err(e) = inner_writer.write_owned_batch(batch) {
                        let err_str = e.to_string();
                        *first_error_clone.lock().unwrap() = Some(err_str.clone());
                        return Err(err_str);
                    }
                }

                // Channel disconnected - finish the file
                inner_writer.finish().map_err(|e| {
                    let err_str = e.to_string();
                    *first_error_clone.lock().unwrap() = Some(err_str.clone());
                    err_str
                })
            })
            .expect("Failed to spawn mzpeak-writer thread");

        Ok(Self {
            sender: Some(sender),
            handle: Some(handle),
            first_error,
        })
    }
}
```

## 9. Write Method

```rust
impl AsyncMzPeakWriter {
    /// Write an owned batch asynchronously.
    ///
    /// The batch is transferred to the background thread via pointer move
    /// (zero-copy). If the channel buffer is full, this method blocks until
    /// space is available (backpressure).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The background thread has already failed (fail-fast)
    /// - The channel is disconnected (background thread exited)
    ///
    /// # Zero-Copy
    ///
    /// The `OwnedColumnarBatch` is moved through the channel. No data bytes
    /// are copied—only the struct's metadata (pointers, lengths, capacities).
    pub fn write_owned_batch(&self, batch: OwnedColumnarBatch) -> Result<(), WriterError> {
        // Fail-fast: check if background thread already errored
        if let Some(err) = self.first_error.lock().unwrap().as_ref() {
            return Err(WriterError::BackgroundWriterError(err.clone()));
        }

        // Get sender (None if finish() was called, but we shouldn't reach here)
        let sender = self.sender.as_ref().ok_or_else(|| {
            WriterError::BackgroundWriterError("Writer already finished".to_string())
        })?;

        // Send batch (blocks if channel is full - backpressure)
        sender.send(batch).map_err(|_| {
            // Channel disconnected - background thread must have exited
            // Check if there's an error message
            let err = self.first_error.lock().unwrap();
            match err.as_ref() {
                Some(msg) => WriterError::BackgroundWriterError(msg.clone()),
                None => WriterError::BackgroundWriterError(
                    "Background writer thread exited unexpectedly".to_string()
                ),
            }
        })
    }
}
```

## 10. Finish Method

```rust
impl AsyncMzPeakWriter {
    /// Finish writing and close the Parquet file.
    ///
    /// This method:
    /// 1. Closes the channel to signal the background thread to stop
    /// 2. Waits for the background thread to complete
    /// 3. Returns the final statistics or propagates any error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The background thread encountered an error during writing
    /// - The background thread panicked
    ///
    /// # Panics
    ///
    /// Does not panic. Thread panics are converted to `WriterError::ThreadPanicked`.
    pub fn finish(mut self) -> Result<WriterStats, WriterError> {
        // Drop sender to signal background thread to stop
        self.sender.take();

        // Wait for background thread to complete
        let handle = self.handle.take().ok_or_else(|| {
            WriterError::BackgroundWriterError("finish() called twice".to_string())
        })?;

        match handle.join() {
            Ok(Ok(stats)) => Ok(stats),
            Ok(Err(err_str)) => Err(WriterError::BackgroundWriterError(err_str)),
            Err(_panic) => Err(WriterError::ThreadPanicked),
        }
    }
}
```

## 11. Drop Safety

```rust
impl Drop for AsyncMzPeakWriter {
    fn drop(&mut self) {
        // If finish() wasn't called, clean up gracefully
        if self.sender.is_some() || self.handle.is_some() {
            // Drop sender to signal thread to stop
            self.sender.take();

            // Wait for thread (don't leave it orphaned)
            if let Some(handle) = self.handle.take() {
                // Log warning - user should call finish() explicitly
                log::warn!(
                    "AsyncMzPeakWriter dropped without calling finish(). \
                     Parquet file may be incomplete."
                );
                // Best-effort join - ignore result since we can't return errors from Drop
                let _ = handle.join();
            }
        }
    }
}
```

## 12. File Changes Summary

| File | Change |
|------|--------|
| `Cargo.toml` | Add `crossbeam-channel = "0.5"` |
| `src/writer/error.rs` | Add `BackgroundWriterError(String)` and `ThreadPanicked` variants |
| `src/writer/config.rs` | Change default compression to `Zstd(3)`, add `async_buffer_capacity: usize` |
| `src/writer/async_writer.rs` | **New file** - `AsyncMzPeakWriter` implementation |
| `src/writer/mod.rs` | Add `mod async_writer; pub use async_writer::AsyncMzPeakWriter;` |

## 13. Dependencies

```toml
# Add to [dependencies] in Cargo.toml
crossbeam-channel = "0.5"
```

## 14. Testing Strategy

### Unit Tests (in `async_writer.rs`)

1. **Basic write/finish cycle** - write batches, verify stats
2. **Backpressure** - verify blocking when channel is full
3. **Error propagation** - simulate write error, verify fail-fast
4. **Drop without finish** - verify warning logged, no panic

### Integration Tests

1. **End-to-end** - write with `AsyncMzPeakWriter`, read back with `MzPeakReader`, verify data integrity
2. **Large file** - write 10M+ peaks, verify no memory explosion

## 15. Performance Expectations

| Metric | Sync Writer | Async Writer | Notes |
|--------|-------------|--------------|-------|
| Throughput | Limited by compression | Near-producer speed | Producer no longer waits |
| Latency | Blocking | Non-blocking until channel full | Backpressure kicks in |
| Memory | O(1) per batch | O(buffer_capacity) batches | Trade-off for throughput |
| CPU | Single thread | Dedicated writer thread | Better core utilization |

## 16. Migration Guide

```rust
// Before (synchronous)
let mut writer = MzPeakWriter::new_file("output.parquet", &metadata, config)?;
for batch in batches {
    writer.write_owned_batch(batch)?;
}
let stats = writer.finish()?;

// After (asynchronous)
let writer = AsyncMzPeakWriter::new(file, metadata, config)?;
for batch in batches {
    writer.write_owned_batch(batch)?;  // Non-blocking until backpressure
}
let stats = writer.finish()?;  // Waits for background thread
```

---

**End of Specification**
