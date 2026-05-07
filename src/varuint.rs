//! LEB128 varuint reader, mirrors `AsyncBinaryReader.read_varuint`.

/// Maximum bytes a u64 takes in LEB128 encoding (ceil(64 / 7)).
pub(crate) const MAX_VARUINT_BYTES: usize = 10;

/// Decode one LEB128 varuint starting at `buf[pos]`. Returns
/// `(value, bytes_consumed)`. Errors on truncation or > 64-bit
/// payload — matches the protocol layer's `ProtocolError` shape.
pub(crate) fn read_varuint(buf: &[u8], pos: usize) -> Result<(u64, usize), VarUintError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut i = pos;
    for _ in 0..MAX_VARUINT_BYTES {
        if i >= buf.len() {
            return Err(VarUintError::Truncated { offset: pos });
        }
        let byte = buf[i];
        value |= ((byte & 0x7F) as u64) << shift;
        i += 1;
        if byte & 0x80 == 0 {
            return Ok((value, i - pos));
        }
        shift += 7;
    }
    Err(VarUintError::Overflow { offset: pos })
}

#[derive(Debug)]
pub(crate) enum VarUintError {
    Truncated { offset: usize },
    Overflow { offset: usize },
}

impl std::fmt::Display for VarUintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VarUintError::Truncated { offset } => {
                write!(f, "varuint truncated at offset {offset}")
            }
            VarUintError::Overflow { offset } => write!(
                f,
                "varuint exceeds {MAX_VARUINT_BYTES} bytes at offset {offset}"
            ),
        }
    }
}
