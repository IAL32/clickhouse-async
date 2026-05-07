//! Compiled Rust acceleration for `clickhouse_async` hot decode paths.
//!
//! The crate is built into a Python extension module (`.so` / `.pyd`)
//! by maturin and lands at `clickhouse_async._fast`. This step (the
//! v0.4 scaffold) ships an *empty* pymodule — subsequent steps each
//! add one function and wire it into the corresponding Python call
//! site:
//!
//! - `decode_strings` — replaces `String.read`'s per-row loop
//! - `transpose` — replaces `Client`'s row-tuple comprehension
//! - `decode_big_int` — replaces `_BigIntCodec.read`'s `int.from_bytes` loop
//!
//! Until those land, the only contract this crate honors is "import
//! works, module is empty". That alone is enough to validate the
//! maturin build pipeline and keep CI green.

use pyo3::prelude::*;

mod decode_strings;
mod varuint;

#[pymodule]
fn _fast(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode_strings::decode_strings, m)?)?;
    Ok(())
}
