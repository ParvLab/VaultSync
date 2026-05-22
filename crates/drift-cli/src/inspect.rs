use clap::Args;

#[derive(Args)]
pub struct InspectArgs {
    pub pid: u32,
    #[arg(long)]
    pub stream: bool,
    #[arg(long)]
    pub state: bool,
}

pub fn run(args: InspectArgs) {
    println!("Drift inspect on pid {} (stream: {}, state: {})", args.pid, args.stream, args.state);
}
