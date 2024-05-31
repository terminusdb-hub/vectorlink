#![allow(non_local_definitions)]

use ::vectorlink_task::{queue::Queue, task::Task};
use pyo3::{exceptions::PyException, prelude::*, types::PyNone};
use serde_json::Value;

#[pyclass(name = "Queue", module = "vectorlink_task")]
struct PyQueue(Queue);

#[pyclass(name = "Task", module = "vectorlink_task")]
struct PyTask(Task);

fn json_as_py(py: Python, data: Option<Value>) -> PyResult<&PyAny> {
    if data.is_none() {
        let none = PyNone::get(py).extract()?;
        return Ok(none);
    }

    let init_data = data.unwrap();

    let init_data = serde_json::to_string(&init_data).unwrap();
    // now turn it into a python dict
    let json = PyModule::import(py, "json")?;
    let loads = json.getattr("loads")?;

    let result = loads.call1((init_data,))?.extract()?;
    Ok(result)
}

#[pymethods]
impl PyQueue {
    #[new]
    fn connect(
        endpoints: Vec<String>,
        service_name: String,
        identity: String,
    ) -> PyResult<PyQueue> {
        let runtime = pyo3_asyncio::tokio::get_runtime();
        runtime.block_on(async {
            let queue = Queue::connect(endpoints, None, service_name, identity)
                .await
                .map_err(|e| PyException::new_err(format!("could not connect: {e}")))?;
            Ok(PyQueue(queue))
        })
    }

    fn next(&mut self) -> PyResult<PyTask> {
        let runtime = pyo3_asyncio::tokio::get_runtime();
        let next_task = runtime
            .block_on(self.0.next_task())
            .map_err(|e| PyException::new_err(format!("could not retrieve next task: {e}")))?;
        Ok(PyTask(next_task))
    }

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.0))
    }
}

#[pymethods]
impl PyTask {
    #[getter]
    fn id(&self) -> PyResult<&str> {
        Ok(self.0.task_id())
    }

    #[getter]
    fn status(&self) -> PyResult<String> {
        Ok(serde_json::to_string(&self.0.status())
            .unwrap()
            .trim_matches('"')
            .to_string())
    }

    #[getter]
    fn init<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let init_data: Option<Value> = self
            .0
            .init()
            .map_err(|e| PyException::new_err(format!("could not retrieve init data: {e}")))?;

        let init_data = json_as_py(py, init_data)?;
        eprintln!("converted!");
        Ok(init_data)
    }

    #[getter]
    fn get_progress<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let progress_data: Option<Value> = self
            .0
            .progress()
            .map_err(|e| PyException::new_err(format!("could not retrieve init data: {e}")))?;

        json_as_py(py, progress_data)
    }

    #[setter]
    fn set_progress<'p>(&mut self, py: Python<'p>, progres: &'p PyAny) -> PyResult<()> {
        let json = PyModule::import(py, "json")?;
        let dumps = json.getattr("dumps")?;
        let progress: String = dumps.call1((progres,))?.extract()?;
        let progress: Value = serde_json::from_str(&progress).unwrap();
        let runtime = pyo3_asyncio::tokio::get_runtime();
        runtime
            .block_on(self.0.set_progress(progress))
            .map_err(|e| PyException::new_err(format!("could not resume task: {e}")))?;

        Ok(())
    }

    #[getter]
    fn result<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let result_data: Option<Value> = self
            .0
            .result()
            .map_err(|e| PyException::new_err(format!("could not retrieve init data: {e}")))?;

        json_as_py(py, result_data)
    }

    #[getter]
    fn error<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let error_data: Option<Value> = self
            .0
            .error()
            .map_err(|e| PyException::new_err(format!("could not retrieve init data: {e}")))?;

        json_as_py(py, error_data)
    }

    fn alive(&mut self) -> PyResult<()> {
        let runtime = pyo3_asyncio::tokio::get_runtime();
        runtime
            .block_on(self.0.alive())
            .map_err(|e| PyException::new_err(format!("could not start task: {e}")))?;

        Ok(())
    }

    fn start(&mut self) -> PyResult<()> {
        let runtime = pyo3_asyncio::tokio::get_runtime();
        runtime
            .block_on(self.0.start())
            .map_err(|e| PyException::new_err(format!("could not start task: {e}")))?;

        Ok(())
    }

    fn resume(&mut self) -> PyResult<()> {
        let runtime = pyo3_asyncio::tokio::get_runtime();
        runtime
            .block_on(self.0.resume())
            .map_err(|e| PyException::new_err(format!("could not resume task: {e}")))?;

        Ok(())
    }

    fn finish<'p>(&mut self, py: Python<'p>, result: &'p PyAny) -> PyResult<()> {
        let json = PyModule::import(py, "json")?;
        let dumps = json.getattr("dumps")?;
        let result: String = dumps.call1((result,))?.extract()?;
        let result: Value = serde_json::from_str(&result).unwrap();
        let runtime = pyo3_asyncio::tokio::get_runtime();
        runtime
            .block_on(self.0.finish(result))
            .map_err(|e| PyException::new_err(format!("could not resume task: {e}")))?;

        Ok(())
    }

    fn finish_error<'p>(&mut self, py: Python<'p>, error: &'p PyAny) -> PyResult<()> {
        let json = PyModule::import(py, "json")?;
        let dumps = json.getattr("dumps")?;
        let error: String = dumps.call1((error,))?.extract()?;
        let error: Value = serde_json::from_str(&error).unwrap();
        let runtime = pyo3_asyncio::tokio::get_runtime();
        runtime
            .block_on(self.0.finish_error(error))
            .map_err(|e| PyException::new_err(format!("could not resume task: {e}")))?;

        Ok(())
    }

    #[pyo3(name = "__repr__")]
    fn repr(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.0))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn vectorlink_task(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyQueue>()?;
    m.add_class::<PyTask>()?;
    Ok(())
}
