//! Command-line entry point for mzPeak conversion and validation.

mod cli;

use clap::Parser;

fn main() -> anyhow::Result<()> {
    let args = cli::Cli::parse();
    cli::init_logging(args.verbosity());
    cli::dispatch(args)
}
