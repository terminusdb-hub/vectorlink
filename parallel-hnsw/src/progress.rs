use std::{any::Any, collections::HashMap};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct LayerStatistics {
    pub node_count: usize,
    pub neighbors: usize,
    pub recall: Option<f32>,
    pub improvement: Option<f32>,
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
    fn centroid_update(&mut self, update: ProgressUpdate) -> Result<(), Interrupt>;
    fn get_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt>;
    fn set_layer_statistics(
        &mut self,
        layer_from_top: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt>;
    fn invalidate_layer_statistics(&mut self, layer_from_top: usize) -> Result<(), Interrupt>;
    fn get_centroid_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt>;
    fn set_centroid_layer_statistics(
        &mut self,
        layer_from_top: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt>;
    fn invalidate_centroid_layer_statistics(
        &mut self,
        layer_from_top: usize,
    ) -> Result<(), Interrupt>;
    fn keep_alive(&mut self) -> Box<dyn Any>;
}

#[derive(Default)]
pub struct SimpleProgressMonitor {
    layer_statistics: HashMap<usize, LayerStatistics>,
    centroid_layer_statistics: HashMap<usize, LayerStatistics>,
}

impl ProgressMonitor for SimpleProgressMonitor {
    fn alive(&mut self) -> Result<(), Interrupt> {
        Ok(())
    }
    fn update(&mut self, _update: ProgressUpdate) -> Result<(), Interrupt> {
        Ok(())
    }

    fn centroid_update(&mut self, _update: ProgressUpdate) -> Result<(), Interrupt> {
        Ok(())
    }

    fn keep_alive(&mut self) -> Box<dyn Any> {
        Box::new(())
    }

    fn set_layer_statistics(
        &mut self,
        layer: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        self.layer_statistics.insert(layer, statistics);
        Ok(())
    }

    fn set_centroid_layer_statistics(
        &mut self,
        layer: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        self.centroid_layer_statistics.insert(layer, statistics);
        Ok(())
    }

    fn get_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        Ok(self.layer_statistics.get(&layer_from_top).copied())
    }

    fn invalidate_layer_statistics(&mut self, layer_from_top: usize) -> Result<(), Interrupt> {
        self.layer_statistics.remove(&layer_from_top);
        Ok(())
    }

    fn get_centroid_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        Ok(self.centroid_layer_statistics.get(&layer_from_top).copied())
    }

    fn invalidate_centroid_layer_statistics(
        &mut self,
        layer_from_top: usize,
    ) -> Result<(), Interrupt> {
        self.centroid_layer_statistics.remove(&layer_from_top);
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

    fn centroid_update(&mut self, update: ProgressUpdate) -> Result<(), Interrupt> {
        (**self).update(update)
    }

    fn keep_alive(&mut self) -> Box<dyn Any> {
        (**self).keep_alive()
    }

    fn set_layer_statistics(
        &mut self,
        layer: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        (**self).set_layer_statistics(layer, statistics)
    }

    fn set_centroid_layer_statistics(
        &mut self,
        layer: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        (**self).set_layer_statistics(layer, statistics)
    }

    fn get_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        (**self).get_layer_statistics(layer_from_top)
    }

    fn invalidate_layer_statistics(&mut self, layer_from_top: usize) -> Result<(), Interrupt> {
        (**self).invalidate_layer_statistics(layer_from_top)
    }

    fn get_centroid_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        (**self).get_centroid_layer_statistics(layer_from_top)
    }

    fn invalidate_centroid_layer_statistics(
        &mut self,
        layer_from_top: usize,
    ) -> Result<(), Interrupt> {
        (**self).invalidate_centroid_layer_statistics(layer_from_top)
    }
}

pub struct PqProgressMonitor<'a> {
    inner: &'a mut dyn ProgressMonitor,
}

impl<'a> PqProgressMonitor<'a> {
    pub fn wrap(inner: &'a mut dyn ProgressMonitor) -> Self {
        Self { inner }
    }
}

impl<'a> ProgressMonitor for PqProgressMonitor<'a> {
    fn alive(&mut self) -> Result<(), Interrupt> {
        self.inner.alive()
    }

    fn update(&mut self, update: ProgressUpdate) -> Result<(), Interrupt> {
        self.inner.centroid_update(update)
    }

    fn centroid_update(&mut self, _update: ProgressUpdate) -> Result<(), Interrupt> {
        panic!("called centroid_update on the PqProgressMonitor");
    }

    fn set_layer_statistics(
        &mut self,
        layer_from_top: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        self.inner
            .set_centroid_layer_statistics(layer_from_top, statistics)
    }

    fn set_centroid_layer_statistics(
        &mut self,
        _layer_from_top: usize,
        _statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        panic!("called set_centroid_layer_statistics on the PqProgressMonitor");
    }

    fn keep_alive(&mut self) -> Box<dyn Any> {
        self.inner.keep_alive()
    }

    fn get_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        self.inner.get_centroid_layer_statistics(layer_from_top)
    }

    fn invalidate_layer_statistics(&mut self, layer_from_top: usize) -> Result<(), Interrupt> {
        self.inner
            .invalidate_centroid_layer_statistics(layer_from_top)
    }

    fn get_centroid_layer_statistics(
        &self,
        _layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        panic!("called get_centroid_layer_statistics on the PqProgressMonitor");
    }

    fn invalidate_centroid_layer_statistics(
        &mut self,
        _layer_from_top: usize,
    ) -> Result<(), Interrupt> {
        panic!("called invalidate_centroid_layer_statistics on the PqProgressMonitor");
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
