pub mod errors;

use std::io;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{self, json};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{
    CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions, SQLOptions,
};

use datafusion::prelude::SessionContext;

#[derive(Debug, Parser)]
#[command(about = "A CLI tool for running SQLs over various data sources.", long_about = None)]
struct DfqArgs {
    #[clap(short, long, default_value = "ansi")]
    dialect: String,
    #[clap(short, long,value_enum, default_value_t=OutputFormat::Terminal)]
    output: OutputFormat,
    /// data sources and SQL, e.g. `sample.csv "select * from t0"`
    data_and_sql: Vec<String>,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum OutputFormat {
    Json,
    JsonArray,
    Csv,
    Parquet,
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
            Some(opt) if opt.ends_with(".csv") || opt.ends_with(".csv.gz") => {
                session_context
                    .register_csv(
                        format!("t{}", idx).as_str(),
                        opt.as_str(),
                        CsvReadOptions::default(),
                    )
                    .await?;
                idx += 1;
            }
            Some(opt) if opt.ends_with(".json") || opt.ends_with(".json.gz") => {
                session_context
                    .register_json(
                        format!("t{}", idx).as_str(),
                        opt.as_str(),
                        NdJsonReadOptions::default(),
                    )
                    .await?;
                idx += 1;
            }
            Some(opt) if opt.ends_with(".parquet") || opt.ends_with(".prq") => {
                session_context
                    .register_parquet(
                        format!("t{}", idx).as_str(),
                        opt.as_str(),
                        ParquetReadOptions::default(),
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
    let dialect = options.dialect.as_str();
    let statement = state.sql_to_statement(query, dialect)?;
    let logical_plan = state.statement_to_plan(statement).await?;

    let sql_options = SQLOptions::new()
        .with_allow_dml(false)
        .with_allow_ddl(false);
    sql_options.verify_plan(&logical_plan)?;

    let optimized = state.optimize(&logical_plan)?;
    let dataframe = session_context.execute_logical_plan(optimized).await?;
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
        OutputFormat::JsonArray => {
            let mut json_writer = json::ArrayWriter::new(io::stdout());
            for batch in dataframe.collect().await? {
                json_writer.write(&batch)?;
            }
            json_writer.finish()?;
        }
        OutputFormat::Json => {
            let mut json_writer = json::LineDelimitedWriter::new(io::stdout());
            for batch in dataframe.collect().await? {
                json_writer.write(&batch)?;
            }
            json_writer.finish()?;
        }
        OutputFormat::Terminal => {
            let results: Vec<RecordBatch> = dataframe.collect().await?;
            let output = arrow::util::pretty::pretty_format_batches(&results)?.to_string();
            println!("{}", output);
        }
        OutputFormat::Parquet => {
            let schema = Schema::from(dataframe.schema());
            let mut writer = ArrowWriter::try_new(io::stdout(), Arc::new(schema), None)?;
            for batch in dataframe.collect().await? {
                writer.write(&batch)?;
            }
            writer.close()?;
        }
    };
    Ok(())
}
