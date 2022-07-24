use fluvio_connectors_common::git_hash_version;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_future::tracing::log::Record;
use fluvio_future::tracing::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schemars::schema_for;
use schemars::JsonSchema;
use std::default::Default;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // =======================================================
    // Get raw params
    // =======================================================

    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "schema": schema_for!(KafkaOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let raw_opts = KafkaOpt::from_args();
    raw_opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Kafka sink connector",
    );

    // =======================================================
    // Setup objects and actors based on opts
    // =======================================================

    // KafkaSinkWorker members need to be inside Arc because it is referenced inside tokio::spawn
    // That expect things moved inside to be static
    // An optimization point for this would be to own the runtime (rather than calling tokio::*)
    // So that runtime::spawn does not need producer to be in Arc
    let kafka_sink_deps = Arc::new(KafkaSinkDependencies::from_kafka_opt(raw_opts).await?);

    info!("Starting stream");

    // =======================================================
    // Preparing Stream from fluvio
    // =======================================================

    let mut stream = kafka_sink_deps
        .common_connector_opt
        .create_consumer_stream()
        .await?;

    // =======================================================
    // Preparing Future Pool Observer
    // =======================================================

    let future_pool_overseer: Arc<tokio::sync::RwLock<FutureMemoryUsageOverseer<_>>> =
        Default::default();
    let memory_usage_highwatermark: usize = 1000; // in bytes
    let memory_usage_inspection_rate = std::time::Duration::from_millis(500);
    let last_memory_usage_inspection = std::time::Instant::now();

    // =======================================================
    // Streaming sequence starts here
    // =======================================================

    loop {
        let memory_usage_highwatermark_passed = {
            let overseer_reader = future_pool_overseer.read().await;
            (&(*overseer_reader)).get_current_memory_usage() >= memory_usage_highwatermark
        };

        if memory_usage_highwatermark_passed
            && last_memory_usage_inspection.elapsed() > memory_usage_inspection_rate
        {
            let mut overseer_writer = future_pool_overseer.write().await;
            overseer_writer.sweep();
        } else {
            match stream.next().await {
                Some(Ok(record)) => {
                    // Clone arcs to be sent to async block's scopes

                    let kafka_sink_deps = kafka_sink_deps.clone();
                    let kafka_topic = kafka_sink_deps.kafka_topic.clone();
                    let kafka_partition = kafka_sink_deps.kafka_partition.clone();
                    let kafka_producer = kafka_sink_deps.kafka_producer.clone();
                    let memory_usage = record.value().len() + std::mem::size_of::<Record>();

                    let future = async move {
                        let mut kafka_record = FutureRecord::to(&(*kafka_topic).as_str())
                            .payload(record.value().clone())
                            .key(record.key().unwrap_or(&[]));
                        if let Some(kafka_partition) = &(*kafka_partition) {
                            kafka_record = kafka_record.partition(*kafka_partition);
                        }
                        let res = kafka_producer
                            .send(kafka_record, Duration::from_secs(0))
                            .await;
                        if let Err(e) = &res {
                            error!("Kafka produce {:?}", e)
                            // TODO: handle error based on the subtype of e
                        }

                        res
                    };
                    let join_handle: JoinHandle<_> = tokio::spawn(future);

                    {
                        let mut overseer_writer = future_pool_overseer.write().await;
                        overseer_writer.push(FutureJoinHandlePair {
                            join_handle,
                            memory_usage,
                        })
                    }
                }
                Some(Err(_error_code)) => {
                    // TODO: explain error from stream
                    // TODO: handle error based on code
                    break;
                }
                None => {
                    // Simply continue
                }
            }
        }
    }

    Ok(())
}

pub struct FutureJoinHandlePair<T> {
    join_handle: JoinHandle<T>,
    memory_usage: usize,
}

pub struct FutureMemoryUsageOverseer<'a, T> {
    /// Pair of JoinHandle and MemoryUsage (a usize)
    pub unfinished_join_handle_pool: Vec<FutureJoinHandlePair<T>>,
    memoized_used_memory: usize,
    _self_lifetime: PhantomData<&'a ()>,
}

impl<'a, T> FutureMemoryUsageOverseer<'a, T> {
    fn push(&mut self, pair: FutureJoinHandlePair<T>) -> () {
        // Push new JoinHandle-MemoryUsage pair
        self.memoized_used_memory += pair.memory_usage;
        self.unfinished_join_handle_pool.push(pair);
    }

    fn sweep(&mut self) -> () {
        // Take finished JoinHandle-MemoryUsage pairs
        let mut old_pairs: Vec<FutureJoinHandlePair<T>> = vec![];
        let mut finished_pairs: Vec<FutureJoinHandlePair<T>> = vec![];

        std::mem::swap(&mut self.unfinished_join_handle_pool, &mut old_pairs);

        old_pairs.into_iter().for_each(|pair| {
            if pair.join_handle.is_finished() {
                finished_pairs.push(pair);
            } else {
                self.unfinished_join_handle_pool.push(pair);
            }
        });

        // Reduced "memoized_used_memory" by records of MemoryUsage from finished pairs
        let freed_memory = finished_pairs
            .into_iter()
            .fold(0, |freed_memory, pair| freed_memory + pair.memory_usage);

        self.memoized_used_memory -= freed_memory;
    }

    fn get_current_memory_usage(&self) -> usize {
        self.memoized_used_memory.clone()
    }
}

impl<'a, T> Default for FutureMemoryUsageOverseer<'a, T> {
    fn default() -> Self {
        FutureMemoryUsageOverseer {
            unfinished_join_handle_pool: Default::default(),
            memoized_used_memory: 0,
            _self_lifetime: Default::default(),
        }
    }
}

#[derive(StructOpt, Debug, JsonSchema, Clone)]
pub struct KafkaOpt {
    /// A Comma separated list of the kafka brokers to connect to
    #[structopt(long, env = "KAFKA_URL", hide_env_values = true)]
    pub kafka_url: String,

    #[structopt(long)]
    pub kafka_topic: Option<String>,

    #[structopt(long)]
    pub kafka_partition: Option<i32>,

    /// A key value pair in the form key:value
    #[structopt(long)]
    pub kafka_option: Vec<String>,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,
}

pub struct KafkaSinkDependencies {
    pub kafka_topic: Arc<String>,
    pub kafka_partition: Arc<Option<i32>>,
    pub kafka_producer: Arc<FutureProducer>,
    pub common_connector_opt: Arc<CommonConnectorOpt>,
}

impl KafkaSinkDependencies {
    async fn from_kafka_opt(opts: KafkaOpt) -> anyhow::Result<KafkaSinkDependencies> {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::config::FromClientConfig;
        let KafkaOpt {
            kafka_url,
            kafka_topic,
            kafka_partition,
            kafka_option,
            common: common_connector_opt,
        } = opts;

        let kafka_topic = kafka_topic.unwrap_or_else(|| common_connector_opt.fluvio_topic.clone());

        let client_config = {
            let mut kafka_options: Vec<(String, String)> = Vec::new();
            for opt in &kafka_option {
                let mut splits = opt.split(':');
                let (key, value) = (
                    splits.next().unwrap().to_string(),
                    splits.next().unwrap().to_string(),
                );
                kafka_options.push((key, value));
            }
            info!("kafka_options: {:?}", kafka_options);

            let mut client_config = ClientConfig::new();
            for (key, value) in &kafka_options {
                client_config.set(key, value);
            }
            client_config.set("bootstrap.servers", kafka_url.clone());
            client_config
        };

        // =======================================================
        // Prepare topic, ensure it exists before creating producer
        // =======================================================

        let admin = AdminClient::from_config(&client_config)?;
        admin
            .create_topics(
                &[NewTopic::new(
                    kafka_topic.as_str(),
                    1,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new(),
            )
            .await?;

        let kafka_producer = client_config.create().expect("Producer creation error");

        Ok(KafkaSinkDependencies {
            kafka_topic: Arc::new(kafka_topic),
            kafka_partition: Arc::new(kafka_partition),
            kafka_producer: Arc::new(kafka_producer),
            common_connector_opt: Arc::new(common_connector_opt),
        })
    }
}
