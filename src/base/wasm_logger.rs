use log::{Level, Log, Metadata, Record, SetLoggerError};

use wasm_bindgen::prelude::*;

// import JS functions from 'worker_logger.js' (in browser binding repo)
#[wasm_bindgen(raw_module = "../worker_logger")]
extern "C" {
    fn log(lvl: &str, file: &str, line: &str, msg: &str);
}

struct WasmLogger;

impl Log for WasmLogger {
    #[inline]
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let lvl = format!("{}", record.level());
        let file =
            format!("{}", record.file().unwrap_or_else(|| record.target()));
        let line = format!(
            "{}",
            record.line().map_or_else(
                || "[unknown]".to_string(),
                |line| line.to_string()
            )
        );
        let msg = format!("{}", record.args());

        log(&lvl, &file, &line, &msg);
    }

    fn flush(&self) {}
}

pub fn init(max_level: Level) -> Result<(), SetLoggerError> {
    let logger = WasmLogger {};
    log::set_boxed_logger(Box::new(logger)).and_then(|_| {
        log::set_max_level(max_level.to_level_filter());
        Ok(())
    })
}
