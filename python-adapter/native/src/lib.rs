//! Bounded-scratch batch decoder. The Python wrapper owns all output arrays.
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    slice,
};
type Result<T> = std::result::Result<T, String>;
fn require(ok: bool, why: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(why.into()) }
}
#[derive(Debug)]
enum Codec {
    Shuffle {
        delta: bool,
        xor: bool,
        first: u64,
        base: u64,
    },
    Packed {
        bits: usize,
    },
    Sparse {
        nnz: usize,
        prefix: usize,
        gap_bits: usize,
        bits: usize,
        width: usize,
        delta: bool,
    },
}
struct Field {
    offset: usize,
    size: usize,
    raw_bytes: usize,
    width: usize,
    codec: Codec,
    quantizer: u8,
    origin: f64,
    step: f64,
}
struct Fragment {
    count: usize,
    source_start: usize,
    fields: [Field; 2],
}
#[derive(Deserialize)]
struct Scan {
    index: usize,
    count: usize,
    fragments: Vec<Fragment>,
    #[serde(default)]
    id: String,
    #[serde(default)]
    representation: String,
    #[serde(default)]
    ms_level: u32,
    #[serde(default)]
    retention_time_minutes: f64,
}
#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum CodecKind {
    Baseline,
    DeltaShuffle,
    Packed,
    ZeroRuns,
    SparseTranspose,
}
#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ShuffleMode {
    Shuffle,
    XorShuffle,
}
#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Prediction {
    None,
    Delta,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum QuantizerKind {
    Linear,
    Log,
    AllZero,
    MassLog,
}
#[derive(Deserialize)]
enum Dtype {
    #[serde(rename = "<u1", alias = "|u1")]
    U1,
    #[serde(rename = "<u2")]
    U2,
    #[serde(rename = "<u4")]
    U4,
    #[serde(rename = "<u8")]
    U8,
}
#[derive(Deserialize)]
struct FieldInput {
    offset: usize,
    size: usize,
    dtype: Dtype,
    codec: CodecInput,
    quantizer: QuantizerInput,
}
#[derive(Deserialize)]
struct CodecInput {
    kind: CodecKind,
    mode: Option<ShuffleMode>,
    first: Option<u64>,
    base: Option<u64>,
    bits: Option<usize>,
    nonzero: Option<usize>,
    prefix_bytes: Option<usize>,
    gap_bits: Option<usize>,
    value_width: Option<usize>,
    prediction: Option<Prediction>,
    raw_bytes: Option<usize>,
}
#[derive(Deserialize)]
struct QuantizerInput {
    kind: QuantizerKind,
    origin: Option<f64>,
    step: Option<f64>,
}
impl<'de> Deserialize<'de> for Fragment {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Input {
            count: usize,
            source_start: usize,
            fields: [FieldInput; 2],
        }
        let v = Input::deserialize(d)?;
        let parse = || -> Result<Self> {
            require(
                v.count <= 16384,
                "Fragment exceeds bounded decoder workspace",
            )?;
            Ok(Self {
                count: v.count,
                source_start: v.source_start,
                fields: [
                    Field::parse(&v.fields[0], v.count)?,
                    Field::parse(&v.fields[1], v.count)?,
                ],
            })
        };
        parse().map_err(serde::de::Error::custom)
    }
}
#[derive(Deserialize)]
struct Manifest {
    spectra: Vec<Scan>,
}
pub struct Decoder {
    scans: Vec<Scan>,
    metadata: Vec<u8>,
    zstd: zstd::bulk::Decompressor<'static>,
}
impl Field {
    fn parse(v: &FieldInput, n: usize) -> Result<Self> {
        let width = match v.dtype {
            Dtype::U1 => 1,
            Dtype::U2 => 2,
            Dtype::U4 => 4,
            Dtype::U8 => 8,
        };
        let c = &v.codec;
        let (codec, raw_bytes) = match c.kind {
            CodecKind::Baseline | CodecKind::DeltaShuffle => {
                let delta = c.kind == CodecKind::DeltaShuffle;
                require(
                    c.mode == Some(ShuffleMode::Shuffle) || c.mode == Some(ShuffleMode::XorShuffle),
                    "Unsupported shuffle mode",
                )?;
                (
                    Codec::Shuffle {
                        delta,
                        xor: c.mode == Some(ShuffleMode::XorShuffle),
                        first: if delta {
                            c.first.ok_or("Missing first")?
                        } else {
                            0
                        },
                        base: if delta {
                            c.base.ok_or("Missing base")?
                        } else {
                            0
                        },
                    },
                    if delta {
                        n.saturating_sub(1) * width
                    } else {
                        n * width
                    },
                )
            }
            CodecKind::Packed => {
                let bits = c.bits.ok_or("Missing bits")?;
                require(bits <= width * 8, "Invalid packed width")?;
                (Codec::Packed { bits }, (n * bits).div_ceil(8))
            }
            CodecKind::ZeroRuns | CodecKind::SparseTranspose => {
                let nnz = c.nonzero.ok_or("Missing nonzero")?;
                let prefix = c.prefix_bytes.ok_or("Missing prefix_bytes")?;
                let gap_bits = c.gap_bits.ok_or("Missing gap_bits")?;
                require(
                    nnz <= n && gap_bits <= 64 && prefix == (nnz * gap_bits).div_ceil(8),
                    "Invalid sparse dimensions",
                )?;
                let transpose = c.kind == CodecKind::SparseTranspose;
                let w = if transpose {
                    c.value_width.ok_or("Missing value_width")?
                } else {
                    0
                };
                let b = if transpose {
                    0
                } else {
                    c.bits.ok_or("Missing bits")?
                };
                require(
                    if transpose {
                        [1, 2, 4, 8].contains(&w)
                            && (c.prediction == Some(Prediction::None)
                                || c.prediction == Some(Prediction::Delta))
                    } else {
                        b <= width * 8
                    },
                    "Invalid sparse values",
                )?;
                (
                    Codec::Sparse {
                        nnz,
                        prefix,
                        gap_bits,
                        bits: b,
                        width: w,
                        delta: c.prediction == Some(Prediction::Delta),
                    },
                    prefix
                        + if transpose {
                            nnz * w
                        } else {
                            (nnz * b).div_ceil(8)
                        },
                )
            }
        };
        if !matches!(codec, Codec::Shuffle { .. }) {
            require(
                c.raw_bytes.ok_or("Missing raw_bytes")? == raw_bytes,
                "Invalid decompressed length",
            )?;
        }
        let q = &v.quantizer;
        let quantizer = match q.kind {
            QuantizerKind::Linear => 0,
            QuantizerKind::Log => 1,
            QuantizerKind::AllZero => 2,
            QuantizerKind::MassLog => 3,
        };
        let (origin, step) = if quantizer == 2 {
            (0., 0.)
        } else {
            (
                q.origin.filter(|x| x.is_finite()).ok_or("Invalid origin")?,
                q.step.filter(|x| x.is_finite()).ok_or("Invalid step")?,
            )
        };
        require(
            quantizer == 2 || (origin > 0. && step > 0.),
            "Invalid quantizer parameters",
        )?;
        Ok(Self {
            offset: v.offset,
            size: v.size,
            raw_bytes,
            width,
            codec,
            quantizer,
            origin,
            step,
        })
    }
}
impl Decoder {
    fn parse(bytes: &[u8]) -> Result<Self> {
        let manifest: Manifest = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        let scans = manifest.spectra;
        for (i, s) in scans.iter().enumerate() {
            require(s.index == i, "Noncontiguous spectrum index")?;
            require(
                matches!(s.representation.as_str(), "" | "profile" | "centroid"),
                "Unsupported spectrum representation",
            )?;
            let mut count = 0usize;
            for f in &s.fragments {
                require(f.source_start == count, "Noncontiguous fragments")?;
                count = count.checked_add(f.count).ok_or("Point count overflow")?;
            }
            require(s.count == count, "Incorrect scan point count")?;
        }
        #[derive(Serialize)]
        struct Metadata<'a> {
            count: Vec<usize>,
            id: Vec<&'a str>,
            representation: Vec<u8>,
            ms_level: Vec<u32>,
            time: Vec<f64>,
        }
        let metadata = serde_json::to_vec(&Metadata {
            count: scans.iter().map(|s| s.count).collect(),
            id: scans.iter().map(|s| s.id.as_str()).collect(),
            representation: scans
                .iter()
                .map(|s| match s.representation.as_str() {
                    "profile" => 1,
                    "centroid" => 2,
                    _ => 0,
                })
                .collect(),
            ms_level: scans.iter().map(|s| s.ms_level).collect(),
            time: scans.iter().map(|s| s.retention_time_minutes).collect(),
        })
        .map_err(|e| e.to_string())?;
        Ok(Self {
            scans,
            metadata,
            zstd: zstd::bulk::Decompressor::new().map_err(|e| e.to_string())?,
        })
    }

    fn decode(
        &mut self,
        signal: &[u8],
        ids: &[u64],
        index: &mut [u64],
        mz: &mut [f64],
        intensity: &mut [f64],
    ) -> Result<()> {
        let mut total = 0usize;
        for &id in ids {
            let s = self
                .scans
                .get(usize::try_from(id).map_err(|_| "Scan index overflow")?)
                .ok_or("Scan index out of range")?;
            total = total.checked_add(s.count).ok_or("Point count overflow")?;
        }
        require(
            total == mz.len() && total == index.len() && total == intensity.len(),
            "Output length mismatch",
        )?;
        let mut raw = Vec::new();
        let mut codes = Vec::new();
        let mut cursor = 0;
        for &id in ids {
            for f in &self.scans[id as usize].fragments {
                let end = cursor + f.count;
                index[cursor..end].fill(id);
                for (field, out) in f
                    .fields
                    .iter()
                    .zip([&mut mz[cursor..end], &mut intensity[cursor..end]])
                {
                    let stop = field
                        .offset
                        .checked_add(field.size)
                        .ok_or("Payload offset overflow")?;
                    let blob = signal
                        .get(field.offset..stop)
                        .ok_or("Field outside signal payload")?;
                    raw.resize(field.raw_bytes, 0);
                    let size = self
                        .zstd
                        .decompress_to_buffer(blob, &mut raw[..])
                        .map_err(|e| e.to_string())?;
                    require(size == field.raw_bytes, "Incorrect decoded length")?;
                    decode_codes(field, f.count, &raw, &mut codes)?;
                    let max = if field.width == 8 {
                        u64::MAX
                    } else {
                        (1u64 << (field.width * 8)) - 1
                    };
                    for (&q, y) in codes.iter().zip(out) {
                        require(
                            q <= max && q <= 9007199254740991,
                            "Code exceeds declared integer range",
                        )?;
                        *y = match field.quantizer {
                            0 => field.origin + field.step * q as f64,
                            1 => {
                                if q == 0 {
                                    0.
                                } else {
                                    field.origin * (field.step * (q as f64 - 1.)).exp()
                                }
                            }
                            2 => 0.,
                            _ => field.origin * (field.step * q as f64).exp(),
                        };
                        require(y.is_finite(), "Nonfinite reconstructed value")?;
                    }
                }
                cursor = end;
            }
        }
        Ok(())
    }
}
fn packed(raw: &[u8], i: usize, bits: usize) -> u64 {
    if bits == 0 {
        return 0;
    }
    let bit = i * bits;
    let start = bit / 8;
    let shift = bit % 8;
    let len = (shift + bits).div_ceil(8);
    let mut word = 0u128;
    for (j, &b) in raw[start..start + len].iter().enumerate() {
        word |= (b as u128) << (j * 8);
    }
    ((word >> shift) & ((1u128 << bits) - 1)) as u64
}
fn shuffled(raw: &[u8], i: usize, n: usize, w: usize) -> u64 {
    let mut q = 0;
    for j in 0..w {
        q |= (raw[j * n + i] as u64) << (8 * j);
    }
    q
}
fn decode_codes(f: &Field, n: usize, raw: &[u8], out: &mut Vec<u64>) -> Result<()> {
    out.clear();
    out.resize(n, 0);
    match f.codec {
        Codec::Shuffle {
            delta,
            xor,
            first,
            base,
        } => {
            let len = if delta { n.saturating_sub(1) } else { n };
            let mut previous = 0;
            let mut cumulative = first;
            if delta && n > 0 {
                out[0] = first;
            }
            for i in 0..len {
                let mut q = shuffled(raw, i, len, f.width);
                if xor {
                    q ^= previous;
                }
                previous = q;
                if delta {
                    cumulative = cumulative
                        .checked_add(base)
                        .and_then(|x| x.checked_add(q))
                        .ok_or("Delta overflow")?;
                    q = cumulative;
                }
                out[i + usize::from(delta)] = q;
            }
        }
        Codec::Packed { bits } => {
            for (i, q) in out.iter_mut().enumerate() {
                *q = packed(raw, i, bits);
            }
        }
        Codec::Sparse {
            nnz,
            prefix,
            gap_bits,
            bits,
            width,
            delta,
        } => {
            let mut pos = 0usize;
            let mut prev = 0i64;
            for i in 0..nnz {
                pos = pos
                    .checked_add(
                        usize::try_from(packed(&raw[..prefix], i, gap_bits))
                            .map_err(|_| "Gap overflow")?,
                    )
                    .ok_or("Gap overflow")?;
                require(pos < n, "Sparse position outside block")?;
                let mut q = if width == 0 {
                    packed(&raw[prefix..], i, bits)
                } else {
                    shuffled(&raw[prefix..], i, nnz, width)
                };
                if delta {
                    let change = (q >> 1) as i64 ^ -((q & 1) as i64);
                    prev = prev.checked_add(change).ok_or("Predictor overflow")?;
                    require(prev >= 0, "Negative intensity code")?;
                    q = prev as u64;
                }
                out[pos] = q;
                pos += 1;
            }
        }
    }
    Ok(())
}
// C ABI callers provide valid, non-overlapping buffers and serialize handle access.
unsafe fn error(dst: *mut u8, cap: usize, msg: &str) {
    if cap > 0 {
        let n = msg.len().min(cap - 1);
        unsafe {
            std::ptr::copy_nonoverlapping(msg.as_ptr(), dst, n);
            *dst.add(n) = 0;
        }
    }
}
/// # Safety
/// JSON and error buffers must be valid for their lengths. Free the returned handle once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mzlab_decoder_new(
    json: *const u8,
    len: usize,
    err: *mut u8,
    cap: usize,
) -> *mut Decoder {
    let result = catch_unwind(AssertUnwindSafe(|| {
        Decoder::parse(unsafe { slice::from_raw_parts(json, len) })
    }));
    match result {
        Ok(Ok(d)) => Box::into_raw(Box::new(d)),
        r => {
            unsafe {
                error(
                    err,
                    cap,
                    &match r {
                        Ok(Err(e)) => e,
                        _ => "Native decoder panic".into(),
                    },
                )
            };
            std::ptr::null_mut()
        }
    }
}
/// # Safety
/// The handle must be null or live, exclusively owned, and created by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mzlab_decoder_free(ptr: *mut Decoder) {
    if !ptr.is_null() {
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }
}
/// # Safety
/// The handle must be live and exclusive. All buffers must be valid for their
/// declared lengths; writable output buffers must not overlap any other buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mzlab_decode(
    ptr: *mut Decoder,
    signal: *const u8,
    size: usize,
    ids: *const u64,
    n: usize,
    index: *mut u64,
    mz: *mut f64,
    intensity: *mut f64,
    count: usize,
    err: *mut u8,
    cap: usize,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| unsafe {
        (&mut *ptr).decode(
            slice::from_raw_parts(signal, size),
            slice::from_raw_parts(ids, n),
            slice::from_raw_parts_mut(index, count),
            slice::from_raw_parts_mut(mz, count),
            slice::from_raw_parts_mut(intensity, count),
        )
    }));
    match result {
        Ok(Ok(())) => 0,
        r => {
            unsafe {
                error(
                    err,
                    cap,
                    &match r {
                        Ok(Err(e)) => e,
                        _ => "Native decoder panic".into(),
                    },
                )
            };
            -1
        }
    }
}

/// # Safety
/// Handle and length pointer must be valid. Returned bytes live until handle destruction.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mzlab_decoder_metadata(ptr: *const Decoder, len: *mut usize) -> *const u8 {
    unsafe {
        *len = (*ptr).metadata.len();
        (*ptr).metadata.as_ptr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_packed_width_and_unaligned_tail() {
        for bits in 0usize..=64 {
            for n in [0, 1, 2, 7, 8, 9, 127] {
                let mask = if bits == 64 {
                    u64::MAX
                } else {
                    (1u64 << bits) - 1
                };
                let codes: Vec<u64> = (0..n)
                    .map(|i| (i as u64).wrapping_mul(0x9e3779b97f4a7c15) & mask)
                    .collect();
                let mut bytes = vec![0u8; (n * bits).div_ceil(8)];
                for (i, &q) in codes.iter().enumerate() {
                    for bit in 0..bits {
                        bytes[(i * bits + bit) / 8] |=
                            (((q >> bit) & 1) as u8) << ((i * bits + bit) % 8);
                    }
                }
                for (i, &q) in codes.iter().enumerate() {
                    assert_eq!(packed(&bytes, i, bits), q);
                }
            }
        }
    }
    #[test]
    fn malformed_manifest_is_rejected() {
        assert!(Decoder::parse(br#"{"spectra":[{"index":1,"count":0,"fragments":[]}]}"#).is_err());
        assert!(Decoder::parse(br#"{"spectra":[{"index":0,"count":1,"fragments":[]}]}"#).is_err());
        assert!(
            Decoder::parse(
                br#"{"spectra":[{"index":0,"count":0,"fragments":[{"count":16385,"fields":[]}]}]}"#
            )
            .is_err()
        );
    }
}
