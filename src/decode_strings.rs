//! Bulk varuint + UTF-8 decode for length-prefixed string columns.
//!
//! The input ``buf`` is the on-wire layout of a String column body —
//! ``[varuint length, body bytes] x n_rows`` packed end-to-end. The
//! Python caller is responsible for assembling that buffer (it can
//! either re-encode parsed varuints or copy raw bytes from the
//! stream); Rust does the entire walk in one tight loop, parsing
//! each varuint, slicing the UTF-8, and constructing one
//! ``PyUnicode`` per row.
//!
//! The win over the prior pure-Python loop is the per-row UTF-8
//! decode + PyUnicode allocation paying no Python-level dispatch
//! cost. Avoiding ``b"".join`` on the body chunks (the lengths-list
//! shape we initially tried) saves a redundant memcpy that
//! dominated for the short-string benchmark.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use crate::varuint::read_varuint;

#[pyfunction]
pub(crate) fn decode_strings(
    py: Python<'_>,
    buf: &Bound<'_, PyBytes>,
    n_rows: usize,
) -> PyResult<Py<PyList>> {
    let bytes = buf.as_bytes();
    let result = PyList::empty_bound(py);
    let mut pos = 0usize;
    for row_idx in 0..n_rows {
        let (len_u64, consumed) = read_varuint(bytes, pos)
            .map_err(|e| PyValueError::new_err(format!("decode_strings row {row_idx}: {e}")))?;
        pos += consumed;
        let len = len_u64 as usize;
        let end = pos.checked_add(len).ok_or_else(|| {
            PyValueError::new_err(format!(
                "decode_strings row {row_idx}: length {len} overflows buffer position"
            ))
        })?;
        if end > bytes.len() {
            return Err(PyValueError::new_err(format!(
                "decode_strings row {row_idx}: needs {len} bytes at offset {pos} \
                 but buf has only {} bytes",
                bytes.len()
            )));
        }
        match std::str::from_utf8(&bytes[pos..end]) {
            Ok(s) => result.append(s)?,
            Err(e) => {
                return Err(PyValueError::new_err(format!(
                    "decode_strings row {row_idx}: invalid UTF-8 at offset {pos}: {e}"
                )));
            }
        }
        pos = end;
    }
    Ok(result.unbind())
}
