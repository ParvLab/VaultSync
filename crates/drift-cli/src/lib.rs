pub mod inspect;
pub mod replay;
pub mod bench;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "drift", about = "Drift sync engine CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Inspect(inspect::InspectArgs),
    Replay(replay::ReplayArgs),
    Bench(bench::BenchArgs),
}

pub fn run() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Inspect(args) => inspect::run(args),
        Commands::Replay(args) => replay::run(args),
        Commands::Bench(args) => bench::run(args),
    }
}
