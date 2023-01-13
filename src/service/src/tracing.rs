use clap::Parser;
use clap::ValueEnum;
use tracing::level_filters::LevelFilter;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for LevelFilter {
    fn from(l: LogLevel) -> Self {
        match l {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
        .into()
    }
}

#[derive(Debug, Clone, Parser)]
pub struct TracingCliArgs {
    #[arg(long, value_enum, default_value = "debug")]
    pub log_level: LogLevel,
}

impl TracingCliArgs {
    pub fn init(&self) -> Result<(), anyhow::Error> {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(self.log_level)
            .finish();

        Ok(tracing::subscriber::set_global_default(subscriber)?)
    }
}
