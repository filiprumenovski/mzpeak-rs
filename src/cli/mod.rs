use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use mzpeak::schema::manifest::Modality;

#[cfg(feature = "mzml")]
mod convert;
#[cfg(feature = "thermo")]
mod convert_thermo;
mod demo;
mod info;
mod validate;

mod config;
mod profile;

pub use profile::Profile;

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

/// Conversion profile for optimizing speed vs compression.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum ProfileArg {
    /// Prioritize speed over compression
    Fast,
    /// Balance between speed and compression
    #[default]
    Balanced,
    /// Maximum compression, slower conversion
    MaxCompression,
}

/// Data modality override for v2 containers.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ModalityArg {
    /// LC-MS (no ion mobility)
    LcMs,
    /// LC-IMS-MS (with ion mobility)
    LcImsMs,
    /// MSI (imaging)
    Msi,
    /// MSI-IMS (imaging with ion mobility)
    MsiIms,
}

impl From<ModalityArg> for Modality {
    fn from(arg: ModalityArg) -> Self {
        match arg {
            ModalityArg::LcMs => Modality::LcMs,
            ModalityArg::LcImsMs => Modality::LcImsMs,
            ModalityArg::Msi => Modality::Msi,
            ModalityArg::MsiIms => Modality::MsiIms,
        }
    }
}

impl From<ProfileArg> for Profile {
    fn from(arg: ProfileArg) -> Self {
        match arg {
            ProfileArg::Fast => Profile::Fast,
            ProfileArg::Balanced => Profile::Balanced,
            ProfileArg::MaxCompression => Profile::MaxCompression,
        }
    }
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

        /// Conversion profile (fast, balanced, max-compression)
        #[arg(short = 'p', long, default_value = "balanced", value_enum)]
        profile: ProfileArg,

        /// Load settings from a TOML config file
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,

        /// Use legacy single-file .mzpeak.parquet format instead of container
        #[arg(long)]
        legacy: bool,

        /// Enable parallel decoding (requires the mzml-parallel feature)
        #[arg(long, default_value_t = false)]
        parallel: bool,

        /// Override modality for v2 containers (auto-detected when omitted)
        #[arg(long, value_enum)]
        modality: Option<ModalityArg>,

        // === Advanced tuning flags (hidden from --help) ===
        /// Compression level for ZSTD (1-22, default: profile-dependent)
        #[arg(short = 'c', long, hide = true)]
        compression_level: Option<i32>,

        /// Row group size (number of peaks per row group)
        #[arg(short = 'r', long, hide = true)]
        row_group_size: Option<usize>,

        /// Batch size for streaming conversion (number of spectra)
        #[arg(short = 'b', long, hide = true)]
        batch_size: Option<usize>,
    },

    /// Convert Thermo RAW file to mzPeak format
    #[cfg(feature = "thermo")]
    ConvertThermo {
        /// Input Thermo RAW file path
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output mzPeak file path (defaults to .mzpeak container format)
        #[arg(value_name = "OUTPUT")]
        output: Option<PathBuf>,

        /// Conversion profile (fast, balanced, max-compression)
        #[arg(short = 'p', long, default_value = "balanced", value_enum)]
        profile: ProfileArg,

        /// Load settings from a TOML config file
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,

        /// Use legacy single-file .mzpeak.parquet format instead of container
        #[arg(long)]
        legacy: bool,

        // === Advanced tuning flags (hidden from --help) ===
        /// Compression level for ZSTD (1-22, default: profile-dependent)
        #[arg(short = 'c', long, hide = true)]
        compression_level: Option<i32>,

        /// Row group size (number of peaks per row group)
        #[arg(short = 'r', long, hide = true)]
        row_group_size: Option<usize>,

        /// Batch size for streaming conversion (number of spectra)
        #[arg(short = 'b', long, hide = true)]
        batch_size: Option<usize>,
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
            profile,
            config,
            legacy,
            parallel,
            modality,
            compression_level,
            row_group_size,
            batch_size,
        } => convert::run(
            input,
            output,
            Profile::from(profile),
            config,
            legacy,
            parallel,
            modality.map(Modality::from),
            compression_level,
            row_group_size,
            batch_size,
        ),
        #[cfg(feature = "thermo")]
        Commands::ConvertThermo {
            input,
            output,
            profile,
            config,
            legacy,
            compression_level,
            row_group_size,
            batch_size,
        } => convert_thermo::run(
            input,
            output,
            Profile::from(profile),
            config,
            legacy,
            compression_level,
            row_group_size,
            batch_size,
        ),
        Commands::Demo {
            output,
            compression_level,
        } => demo::run(output, compression_level),
        Commands::Info { file } => info::run(file),
        Commands::Validate { file } => validate::run(file),
    }
}
