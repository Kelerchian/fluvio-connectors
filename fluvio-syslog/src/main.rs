use structopt::StructOpt;

mod error;
mod produce;
mod consume;

use error::ConnectorError;
use produce::ProducerOpts;
use consume::ConsumerOpts;

const DEFAULT_TOPIC: &str = "syslog";

#[derive(StructOpt, Debug)]
enum ConnectorOpts {
    #[structopt(name = "produce")]
    Produce(ProducerOpts),

    #[structopt(name = "consume")]
    Consume(ConsumerOpts),
}


#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ConnectorError> {
    let opts = ConnectorOpts::from_args();
    match opts {
        ConnectorOpts::Produce(opts) => {
            let _ = opts.exec().await?;
        },
        ConnectorOpts::Consume(opts) => {
            let _ = opts.exec().await?;
        }
    }

    Ok(())
}
