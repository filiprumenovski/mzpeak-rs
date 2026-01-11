use serde::{Deserialize, Serialize};

/// Pressure trace over time (e.g., pump pressure during LC run)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PressureTrace {
    /// Name/identifier (e.g., "Pump A", "Column Pressure")
    pub name: String,

    /// Unit for pressure values
    pub unit: String,

    /// Time points in minutes
    pub times_min: Vec<f64>,

    /// Pressure values
    pub values: Vec<f64>,
}

/// Temperature trace over time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemperatureTrace {
    /// Name/identifier (e.g., "Column Oven", "Autosampler")
    pub name: String,

    /// Time points in minutes
    pub times_min: Vec<f64>,

    /// Temperature values in Celsius
    pub values_celsius: Vec<f64>,
}
