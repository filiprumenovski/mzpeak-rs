use arrow::array::{
    Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray,
    StringArray,
};
use arrow::record_batch::RecordBatch;

use super::ReaderError;

/// Get a required Int64 column by name.
pub(super) fn get_int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Int64", name)))
}

/// Get a required Int16 column by name.
pub(super) fn get_int16_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int16Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<Int16Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Int16", name)))
}

/// Get a required Int8 column by name.
pub(super) fn get_int8_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int8Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Int8", name)))
}

/// Get a required Float32 column by name.
pub(super) fn get_float32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float32Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float32", name)))
}

/// Get a required Float64 column by name.
pub(super) fn get_float64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float64Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float64", name)))
}

/// Get an optional Float64 column by name.
pub(super) fn get_optional_float64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a Float64Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<Float64Array>()
}

/// Get an optional Float32 column by name.
pub(super) fn get_optional_float32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a Float32Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<Float32Array>()
}

/// Get an optional Int16 column by name.
pub(super) fn get_optional_int16_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a Int16Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<Int16Array>()
}

/// Get an optional Int32 column by name.
pub(super) fn get_optional_int32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a Int32Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<Int32Array>()
}

/// Read an optional f64 value from a nullable array.
pub(super) fn get_optional_f64(array: Option<&Float64Array>, idx: usize) -> Option<f64> {
    array.and_then(|arr| if arr.is_null(idx) { None } else { Some(arr.value(idx)) })
}

/// Read an optional f32 value from a nullable array.
pub(super) fn get_optional_f32(array: Option<&Float32Array>, idx: usize) -> Option<f32> {
    array.and_then(|arr| if arr.is_null(idx) { None } else { Some(arr.value(idx)) })
}

/// Read an optional i16 value from a nullable array.
pub(super) fn get_optional_i16(array: Option<&Int16Array>, idx: usize) -> Option<i16> {
    array.and_then(|arr| if arr.is_null(idx) { None } else { Some(arr.value(idx)) })
}

/// Read an optional i32 value from a nullable array.
pub(super) fn get_optional_i32(array: Option<&Int32Array>, idx: usize) -> Option<i32> {
    array.and_then(|arr| if arr.is_null(idx) { None } else { Some(arr.value(idx)) })
}

/// Get a required String column by name.
pub(super) fn get_string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not String", name)))
}

/// Get a required List column by name.
pub(super) fn get_list_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a ListArray, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not List", name)))
}

/// Extract a f64 list from a list array row.
pub(super) fn extract_f64_list(list_array: &ListArray, idx: usize) -> Vec<f64> {
    let values = list_array.values();
    let float_array = values.as_any().downcast_ref::<Float64Array>().unwrap();
    let start = list_array.value_offsets()[idx] as usize;
    let end = list_array.value_offsets()[idx + 1] as usize;
    (start..end).map(|i| float_array.value(i)).collect()
}

/// Extract a f32 list from a list array row.
pub(super) fn extract_f32_list(list_array: &ListArray, idx: usize) -> Vec<f32> {
    let values = list_array.values();
    let float_array = values.as_any().downcast_ref::<Float32Array>().unwrap();
    let start = list_array.value_offsets()[idx] as usize;
    let end = list_array.value_offsets()[idx + 1] as usize;
    (start..end).map(|i| float_array.value(i)).collect()
}
