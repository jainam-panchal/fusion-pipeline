//! Pipeline binary entry point. NATS wiring lands in a later ticket.

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> anyhow::Result<()> {
    println!("fusion-pipeline: NATS source and sink are not wired yet; see docs/specs");
    Ok(())
}
