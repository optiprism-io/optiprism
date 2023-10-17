pub trait Sink {
    fn track(&self, track: track::Track) -> Result<()>;
}
