//! Convert, inspect and validate v2 datasets.
use clap::{Parser, Subcommand, ValueEnum};
use mzpeak::{
    convert::{convert, ConvertOptions},
    reader::MzPeakReader,
    writer::CompressionType,
};
use std::path::PathBuf;
#[derive(Parser)]
#[command(
    name = "mzpeak-convert",
    version,
    about = "Streaming v2 mass-spectrometry conversion"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Clone, Copy, ValueEnum)]
enum Profile {
    Fast,
    Balanced,
    MaxCompression,
}
#[derive(Subcommand)]
enum Command {
    #[command(alias = "convert-thermo")]
    Convert {
        input: PathBuf,
        output: PathBuf,
        #[arg(long, value_enum, default_value = "balanced")]
        profile: Profile,
        #[arg(long)]
        compression_level: Option<i32>,
        #[arg(long, default_value_t = 256)]
        batch_size: usize,
        #[arg(long)]
        rawr_binary: Option<PathBuf>,
    },
    Info {
        file: PathBuf,
    },
    Validate {
        file: PathBuf,
    },
}
fn main() -> anyhow::Result<()> {
    let result = match Cli::parse().command {
        Command::Convert {
            input,
            output,
            profile,
            compression_level,
            batch_size,
            rawr_binary,
        } => {
            let level = compression_level.unwrap_or(match profile {
                Profile::Fast => 1,
                Profile::Balanced => 3,
                Profile::MaxCompression => 15,
            });
            let mut options = ConvertOptions {
                batch_size,
                rawr_binary,
                ..Default::default()
            };
            options.writer_config.compression = CompressionType::Zstd(level);
            serde_json::to_value(convert(input, output, &options)?)?
        }
        Command::Info { file } => {
            let reader = MzPeakReader::open(file)?;
            serde_json::json!({"manifest":reader.manifest(),"metadata":reader.metadata()})
        }
        Command::Validate { file } => {
            serde_json::to_value(mzpeak::validator::validate_mzpeak_file(file)?)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
