use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[cfg(feature = "mzml")]
mod convert;
mod demo;
mod info;
mod validate;

/// mzPeak - Modern Mass Spectrometry Data Format Converter
#[derive(Parser)]
#[command(name = "mzpeak")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Verbosity level (-v for info, -vv for debug)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert mzML file to mzPeak format
    #[cfg(feature = "mzml")]
    Convert {
        /// Input mzML file path
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output mzPeak file path (defaults to .mzpeak container format)
        #[arg(value_name = "OUTPUT")]
        output: Option<PathBuf>,

        /// Use legacy single-file .mzpeak.parquet format instead of container
        #[arg(long)]
        legacy: bool,

        /// Compression level for ZSTD (1-22, default: 3)
        #[arg(short = 'c', long, default_value = "3")]
        compression_level: i32,

        /// Row group size (number of peaks per row group)
        #[arg(short = 'r', long, default_value = "100000")]
        row_group_size: usize,

        /// Batch size for streaming conversion (number of spectra)
        #[arg(short = 'b', long, default_value = "1000")]
        batch_size: usize,

        /// Enable parallel decoding (requires the parallel-decode feature)
        #[arg(long, default_value_t = false)]
        parallel: bool,
    },

    /// Generate demo LC-MS data for testing
    Demo {
        /// Output mzPeak file path
        #[arg(value_name = "OUTPUT", default_value = "demo_lcms_run.mzpeak.parquet")]
        output: PathBuf,

        /// Compression level for ZSTD (1-22, default: 3)
        #[arg(short = 'c', long, default_value = "3")]
        compression_level: i32,
    },

    /// Display information about an mzPeak file
    Info {
        /// Input mzPeak file path
        #[arg(value_name = "FILE")]
        file: PathBuf,
    },

    /// Validate mzPeak file integrity and compliance
    Validate {
        /// Input mzPeak file or directory path
        #[arg(value_name = "FILE")]
        file: PathBuf,
    },
}

impl Cli {
    pub fn verbosity(&self) -> u8 {
        self.verbose
    }
}

pub fn init_logging(verbosity: u8) {
    let log_level = match verbosity {
        0 => "warn",
        1 => "info",
        _ => "debug",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();
}

pub fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        #[cfg(feature = "mzml")]
        Commands::Convert {
            input,
            output,
            legacy,
            compression_level,
            row_group_size,
            batch_size,
            parallel,
        } => convert::run(
            input,
            output,
            legacy,
            compression_level,
            row_group_size,
            batch_size,
            parallel,
        ),
        Commands::Demo {
            output,
            compression_level,
        } => demo::run(output, compression_level),
        Commands::Info { file } => info::run(file),
        Commands::Validate { file } => validate::run(file),
    }
}
