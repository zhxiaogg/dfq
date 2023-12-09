pub mod errors;

use std::io;

use clap::{Parser, ValueEnum};
use datafusion::arrow::csv;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{self, json};
use datafusion::prelude::{CsvReadOptions, DataFrame, SQLOptions};

use datafusion::prelude::SessionContext;

#[derive(Debug, Parser)]
#[command(about = "A CLI for running SQLs over various data sources.", long_about = None)]
struct DfqArgs {
    #[clap(short, long)]
    dialect: Option<String>,
    #[clap(short, long,value_enum, default_value_t=OutputFormat::Terminal)]
    output: OutputFormat,
    /// data sources and SQL, e.g. `sample.csv "select * from t0"`
    data_and_sql: Vec<String>,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum OutputFormat {
    Json,
    Csv,
    Terminal,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = DfqArgs::parse();
    if args.data_and_sql.is_empty() {
        panic!()
    }
    let session_context = SessionContext::new();
    let mut options = args.data_and_sql.iter();
    let mut idx = 0;
    loop {
        match options.next() {
            Some(opt) if opt.ends_with(".csv") => {
                session_context
                    .register_csv(
                        format!("t{}", idx).as_str(),
                        opt.as_str(),
                        CsvReadOptions::default(),
                    )
                    .await?;
                idx += 1;
            }
            Some(opt) => {
                let dataframe = execute_statement(&session_context, opt.as_str(), &args).await?;
                print(dataframe, &args).await?;
                return Ok(());
            }
            None => {
                panic!();
            }
        }
    }
}

async fn execute_statement(
    session_context: &SessionContext,
    query: &str,
    options: &DfqArgs,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let state = session_context.state();
    let dialect = options.dialect.clone().unwrap_or("generic".to_string());
    let statement = state.sql_to_statement(query, dialect.as_str())?;
    let logical_plan = state.statement_to_plan(statement).await?;

    let sql_options = SQLOptions::new()
        .with_allow_dml(false)
        .with_allow_ddl(false);
    sql_options.verify_plan(&logical_plan)?;

    let dataframe = session_context.execute_logical_plan(logical_plan).await?;
    Ok(dataframe)
}

async fn print(dataframe: DataFrame, options: &DfqArgs) -> Result<(), Box<dyn std::error::Error>> {
    match options.output {
        OutputFormat::Csv => {
            let mut csv_writer = csv::Writer::new(io::stdout());
            for batch in dataframe.collect().await? {
                csv_writer.write(&batch)?;
            }
        }
        OutputFormat::Json => {
            let results: Vec<RecordBatch> = dataframe.collect().await?;
            let mut json_writer = json::ArrayWriter::new(io::stdout());
            json_writer.write_batches(&results.iter().collect::<Vec<_>>())?;
            json_writer.finish()?;
        }
        OutputFormat::Terminal => {
            let results: Vec<RecordBatch> = dataframe.collect().await?;
            let output = arrow::util::pretty::pretty_format_batches(&results)?.to_string();
            println!("{}", output);
        }
    };
    Ok(())
}
