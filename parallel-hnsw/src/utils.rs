#[macro_export]
macro_rules! timeit {
    ($e:expr) => {{
        let e_str = stringify!($e);
        let start_time = chrono::Local::now();
        let result = $e;
        let end_time = chrono::Local::now();
        let elapsed_time = end_time - start_time;
        eprintln!("{elapsed_time}: {e_str}");

        result
    }};
}
