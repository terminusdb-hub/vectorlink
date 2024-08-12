use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use vectorlink_task::task::{TaskHandler, TaskLiveness};

#[derive(Clone, Serialize, Deserialize)]
pub struct CollationRequest {
    domain: usize,
    commit: String,
    directory: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CollationProgress {
    block: usize,
}

pub struct CollationTaskHandler;

#[async_trait]
impl TaskHandler for CollationTaskHandler {
    type Init = CollationRequest;

    type Progress = CollationProgress;

    type Complete = ();

    type Error = String;

    async fn initialize(
        _live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Progress, Self::Error> {
        Ok(CollationProgress { block: 0 })
    }

    async fn process(
        mut _live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Complete, Self::Error> {
        // Read file from EFS
        // Load fragment of vectors in addition to index into memory
        // perform matrix product

        todo!();
    }
}
