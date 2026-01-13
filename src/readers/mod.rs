//! Reader utilities for vendor backends (TDF streaming and raw frame capture).
#[cfg(feature = "tdf")]
pub mod raw_tdf_frame;
#[cfg(feature = "tdf")]
pub mod tdf_streamer;

#[cfg(feature = "tdf")]
pub use raw_tdf_frame::RawTdfFrame;
#[cfg(feature = "tdf")]
pub use tdf_streamer::{FramePartition, TdfStreamer};
