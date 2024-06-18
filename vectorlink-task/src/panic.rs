use lazy_static::lazy_static;
use std::{
    backtrace::Backtrace,
    cell::RefCell,
    collections::HashMap,
    future::Future,
    panic::{set_hook, take_hook},
    pin::pin,
    sync::{Arc, Mutex},
};

thread_local! {
    static CURRENT_TASK: RefCell<Option<String>> = RefCell::new(None);
}

lazy_static! {
    static ref LAST_ERRORS: Arc<Mutex<HashMap<String, String>>> = Default::default();
}

/// Installs the tasking panic hook.
/// On panic, this will check the thread-local context to see if we're
/// currently polling a TaskFuture (meaning we're in the init or
/// process functions of a task), and if so, it'll force capture a
/// backtrace and store it globally.
pub fn set_panic_hook() {
    let old_hook = take_hook();
    set_hook(Box::new(move |info| {
        CURRENT_TASK.with(|t| {
            /*
            if let Some(t) = t.borrow().as_ref() {
                let msg = if let Some(p) = info.payload().downcast_ref::<&str>() {
                    p
                } else if let Some(p) = info.payload().downcast_ref::<String>() {
                    &p
                } else {
                    "unknown error"
                };
                let bt = Backtrace::force_capture();
                let error = format!("panic while running task {t}: {msg}\n{bt}");
                eprintln!("{}", error);

                let mut error_map = LAST_ERRORS.lock().expect("could not retrieve error map!");
                error_map.insert(t.clone(), error);
            } else {
            */
            old_hook(info);
            //}
        })
    }));
}

struct TaskFuture<T, F: Future<Output = T>> {
    task_id: String,
    inner: F,
}

impl<T, F: Future<Output = T> + Unpin> Future for TaskFuture<T, F> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // set current task
        CURRENT_TASK.set(Some(self.task_id.clone()));

        // clear the error
        let mut error_map = LAST_ERRORS.lock().expect("errormap write failed?");
        error_map.remove(&self.task_id);

        // set up canary to clear current task
        // When it drops (either due to succesful end of poll, or a
        // panic), it'll clear the current task so we don't
        // accidentally interpret later panics as being part of some
        // task.
        let _canary = TaskCanary;

        // call original future
        pin!(&mut self.inner).poll(cx)
    }
}

pub struct TaskCanary;

impl Drop for TaskCanary {
    fn drop(&mut self) {
        CURRENT_TASK.set(None);
    }
}

pub async fn catch_panic<F: Future<Output = R> + Send + Unpin + 'static, R: Send + 'static>(
    task_id: String,
    future: F,
) -> Result<R, String> {
    let handle = tokio::spawn(TaskFuture {
        task_id: task_id.clone(),
        inner: Box::new(future),
    });
    //let handle = tokio::spawn(future);
    eprintln!("before awaiting the handle");
    let result = handle.await;
    eprintln!("after awaiting the handle");
    match result {
        Ok(r) => {
            eprintln!("catch_panic detected no error");
            Ok(r)
        }
        Err(_e) => {
            eprintln!("catch_panic detected an error!");
            // the task panicked. Time to retrieve the error from the global hashmap.
            let mut error_map = LAST_ERRORS.lock().expect("could not read error map!");
            if let Some(error) = error_map.remove(&task_id) {
                eprintln!("the error was in the error map!");
                Err(error)
            } else {
                eprintln!("the error was not in the error map!");
                Err("task errored but no error was set".to_string())
            }
        }
    }
}
