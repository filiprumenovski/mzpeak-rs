/// Output mode for the dataset writer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Directory-based bundle (legacy)
    Directory,
    /// Single ZIP container file (default)
    Container,
}
