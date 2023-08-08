use okaywal::WriteAheadLog;

#[derive(Debug, Clone)]
pub struct OptiWal(pub WriteAheadLog);
