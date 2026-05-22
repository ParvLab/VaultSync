use clap::Args;

#[derive(Args)]
pub struct ReplayArgs {
    pub trace_file: String,
}

pub fn run(args: ReplayArgs) {
    println!("Drift replay from {}", args.trace_file);
}
