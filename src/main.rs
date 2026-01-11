//! # mzPeak Converter
//!
//! A command-line tool for converting mass spectrometry data to the mzPeak format.
//!
//! ## Supported Input Formats
//!
//! - **mzML**: HUPO-PSI standard XML format (via streaming parser)
//! - **Demo**: Generate mock LC-MS data for testing
//!
//! ## Usage
//!
//! ```bash
//! # Convert mzML to mzPeak
//! mzpeak convert input.mzML output.mzpeak.parquet
//!
//! # Generate demo data
//! mzpeak demo output.mzpeak.parquet
//! ```

mod cli;

use anyhow::Result;
use clap::Parser;

fn main() -> Result<()> {
    let cli = cli::Cli::parse();
    cli::init_logging(cli.verbosity());
    cli::dispatch(cli)
}
