//! # List SPU Groups CLI
//!
//! CLI tree and processing to list SPU Groups
//!

use std::sync::Arc;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::managed_connector::{
    ManagedConnectorSpec,
};

use fluvio_extension_common::Terminal;
use fluvio_extension_common::OutputFormat;
use crate::error::ConnectorError as ClusterCliError;

#[derive(Debug, StructOpt)]
pub struct ListManagedSpuGroupsOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListManagedSpuGroupsOpt {
    /// Process list spus cli request
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<(), ClusterCliError> {
        let admin = fluvio.admin().await;
        let lists = admin.list::<ManagedConnectorSpec, _>(vec![]).await?;

        output::spu_group_response_to_output(out, lists, self.output.format)
    }
}

mod output {

    //!
    //! # Fluvio SC - output processing
    //!
    //! Format SPU Group response based on output type

    use prettytable::Row;
    use prettytable::row;
    use prettytable::Cell;
    use prettytable::cell;
    use prettytable::format::Alignment;
    use tracing::debug;
    use serde::Serialize;
    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;

    use fluvio::metadata::objects::Metadata;
    use fluvio_controlplane_metadata::managed_connector::ManagedConnectorSpec;

    use crate::error::ConnectorError as ClusterCliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListManagedConnectors(Vec<Metadata<ManagedConnectorSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SPU Group based on output type
    pub fn spu_group_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_spu_groups: Vec<Metadata<ManagedConnectorSpec>>,
        output_type: OutputType,
    ) -> Result<(), ClusterCliError> {
        debug!("managed connectors: {:#?}", list_spu_groups);

        if !list_spu_groups.is_empty() {
            let groups = ListManagedConnectors(list_spu_groups);
            out.render_list(&groups, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no managed connectors");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListManagedConnectors {
        /// table header implementation
        fn header(&self) -> Row {
            row!["NAME", "STATUS",]
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            self.0.iter().map(|_g| "".to_owned()).collect()
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|r| {
                    let spec = &r.spec;
                    Row::new(vec![
                        Cell::new_align(&r.name, Alignment::RIGHT),
                        Cell::new_align(&r.status.to_string(), Alignment::RIGHT),
                    ])
                })
                .collect()
        }
    }
}
