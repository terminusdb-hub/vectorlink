use std::any::Any;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct LayerStatistics {
    recall: Option<f32>,
    node_count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ProgressUpdate {
    // TODO: this should probably not be an arbitrary json value
    pub state: serde_json::Value,
}
#[derive(Debug, Error)]
#[error("interrupted")]
pub struct Interrupt;

pub trait ProgressMonitor: Send {
    fn alive(&mut self) -> Result<(), Interrupt>;
    fn update(&mut self, update: ProgressUpdate) -> Result<(), Interrupt>;
    fn layer_statistics(
        &mut self,
        layer: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt>;
    fn keep_alive(&mut self) -> Box<dyn Any>;
}

impl ProgressMonitor for () {
    fn alive(&mut self) -> Result<(), Interrupt> {
        Ok(())
    }
    fn update(&mut self, _update: ProgressUpdate) -> Result<(), Interrupt> {
        Ok(())
    }

    fn keep_alive(&mut self) -> Box<dyn Any> {
        Box::new(())
    }

    fn layer_statistics(
        &mut self,
        _layer: usize,
        _statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        Ok(())
    }
}

impl ProgressMonitor for Box<dyn ProgressMonitor> {
    fn alive(&mut self) -> Result<(), Interrupt> {
        (**self).alive()
    }
    fn update(&mut self, update: ProgressUpdate) -> Result<(), Interrupt> {
        (**self).update(update)
    }

    fn keep_alive(&mut self) -> Box<dyn Any> {
        (**self).keep_alive()
    }

    fn layer_statistics(
        &mut self,
        layer: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        (**self).layer_statistics(layer, statistics)
    }
}

#[macro_export]
macro_rules! keepalive {
    ($live: expr, $body: expr) => {{
        {
            let _guard = $live.keep_alive();
            $body
        }
    }};
}
