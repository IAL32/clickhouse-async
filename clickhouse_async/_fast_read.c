/*
 * _fast_read.c — C-accelerated read paths for clickhouse-async hot codecs.
 *
 * This file is compiled as an *optional* extension. The Python codecs
 * try `from clickhouse_async import _fast_read` and fall back to their
 * pure-Python implementations when the extension isn't loadable, so a
 * bare install with no C compiler still imports cleanly.
 *
 * Limited API (Py_LIMITED_API = 0x030B0000) so a single .abi3.so wheel
 * covers Python 3.11+ across the supported platforms — same shape as
 * the matrix we pin in pyproject.toml.
 *
 * Note on the datetime C API: the `<datetime.h>` macros (`PyDateTime_IMPORT`,
 * `PyDateTime_FromDateAndTime`, etc.) only joined the limited API in
 * Python 3.13. For 3.11+ ABI3 we go through the public Python interface
 * — cache `datetime.datetime` at module init and `PyObject_CallFunction`
 * it per row. Same end result; one extra dispatch we'd otherwise skip.
 */

#define Py_LIMITED_API 0x030B0000
#include <Python.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#define MODULE_NAME "_fast_read"
#define MODULE_VERSION "0.5.0-dev"

/* Cached at module init for the lifetime of the process. */
static PyObject *_datetime_cls = NULL;
static PyObject *_datetime_fromtimestamp = NULL;

static PyObject *
fast_read_available(PyObject *self, PyObject *args)
{
    Py_RETURN_TRUE;
}

/*
 * decode_datetime(buf: bytes, n_rows: int, tzinfo: object | None)
 *     -> list[datetime]
 *
 * Reads `n_rows` little-endian UInt32 timestamps from `buf` and returns
 * a list of `datetime.datetime` objects. `tzinfo=None` produces naive
 * datetimes (the `gmtime_r` interpretation of the epoch seconds).
 * Anything else is passed straight to the datetime constructor as the
 * `tzinfo` kwarg, producing aware datetimes in that timezone.
 *
 * Equivalent pure-Python (the path we replace):
 *
 *     timestamps = struct.unpack(f"<{n_rows}I", buf[:4 * n_rows])
 *     from_ts = datetime.fromtimestamp
 *     if tzinfo is not None:
 *         return [from_ts(ts, tz=tzinfo) for ts in timestamps]
 *     return [from_ts(ts, tz=UTC).replace(tzinfo=None) for ts in timestamps]
 *
 * The naive path saves a `.replace(tzinfo=None)` call per row by going
 * directly from `gmtime_r` components to a naive datetime — that's the
 * win step 3 lands. Aware tracks roughly 1.5x faster purely from the
 * shorter call chain (no Python frame per row).
 */
static PyObject *
fast_read_decode_datetime(PyObject *self, PyObject *args)
{
    Py_buffer buffer;
    Py_ssize_t n_rows;
    PyObject *tzinfo;

    if (!PyArg_ParseTuple(args, "y*nO", &buffer, &n_rows, &tzinfo)) {
        return NULL;
    }

    if (n_rows < 0) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError, "n_rows must be non-negative");
        return NULL;
    }

    Py_ssize_t needed = n_rows * 4;
    if (buffer.len < needed) {
        PyBuffer_Release(&buffer);
        PyErr_Format(
            PyExc_ValueError,
            "buffer too short: need %zd bytes, got %zd",
            needed, buffer.len);
        return NULL;
    }

    PyObject *result = PyList_New(n_rows);
    if (result == NULL) {
        PyBuffer_Release(&buffer);
        return NULL;
    }

    const int aware = (tzinfo != Py_None);
    const uint8_t *data = (const uint8_t *)buffer.buf;

    for (Py_ssize_t i = 0; i < n_rows; i++) {
        const Py_ssize_t base = i * 4;
        const uint32_t ts =
            ((uint32_t)data[base])
            | ((uint32_t)data[base + 1] << 8)
            | ((uint32_t)data[base + 2] << 16)
            | ((uint32_t)data[base + 3] << 24);

        PyObject *dt;
        if (aware) {
            /*
             * Aware: defer to `datetime.fromtimestamp(ts, tz)`. We
             * could build a UTC datetime via gmtime_r and then
             * `.astimezone(tz)`, but that's two object allocations
             * per row vs one — and `fromtimestamp` already routes to
             * the C-level `_PyTime_localtime` path internally. The
             * win over pure Python here is the lack of an
             * interpreter frame per row, not skipping
             * `fromtimestamp` itself.
             */
            PyObject *ts_obj = PyLong_FromUnsignedLong(ts);
            if (ts_obj == NULL) {
                Py_DECREF(result);
                PyBuffer_Release(&buffer);
                return NULL;
            }
            dt = PyObject_CallFunctionObjArgs(
                _datetime_fromtimestamp, ts_obj, tzinfo, NULL);
            Py_DECREF(ts_obj);
        } else {
            /*
             * Naive: gmtime_r the epoch seconds and hand the
             * components straight to `datetime(...)` — one call,
             * one allocation, no aware-then-strip dance the pure-
             * Python path is forced into.
             */
            const time_t t = (time_t)ts;
            struct tm tm_;
#ifdef _WIN32
            if (gmtime_s(&tm_, &t) != 0) {
#else
            if (gmtime_r(&t, &tm_) == NULL) {
#endif
                Py_DECREF(result);
                PyBuffer_Release(&buffer);
                PyErr_Format(
                    PyExc_ValueError,
                    "invalid timestamp at row %zd: %u", i, ts);
                return NULL;
            }
            dt = PyObject_CallFunction(
                _datetime_cls, "iiiiii",
                tm_.tm_year + 1900, tm_.tm_mon + 1, tm_.tm_mday,
                tm_.tm_hour, tm_.tm_min, tm_.tm_sec);
        }

        if (dt == NULL) {
            Py_DECREF(result);
            PyBuffer_Release(&buffer);
            return NULL;
        }
        /* PyList_SetItem steals the reference on success. */
        if (PyList_SetItem(result, i, dt) < 0) {
            /* On failure SetItem itself decrefs `dt`. */
            Py_DECREF(result);
            PyBuffer_Release(&buffer);
            return NULL;
        }
    }

    PyBuffer_Release(&buffer);
    return result;
}

static PyMethodDef FastReadMethods[] = {
    {"available", fast_read_available, METH_NOARGS,
     "Return True. Smoke test that the C extension was built and loaded."},
    {"decode_datetime", fast_read_decode_datetime, METH_VARARGS,
     "decode_datetime(buf, n_rows, tzinfo) -> list[datetime].\n\n"
     "Decode a column of UInt32 LE Unix timestamps into a list of datetime\n"
     "objects. Pass None for naive UTC, a tzinfo for aware."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef fast_read_module = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME,
    "C-accelerated read paths for clickhouse-async hot codecs.",
    -1,
    FastReadMethods,
    NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC
PyInit__fast_read(void)
{
    PyObject *m = PyModule_Create(&fast_read_module);
    if (m == NULL) {
        return NULL;
    }
    if (PyModule_AddStringConstant(m, "__version__", MODULE_VERSION) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    /* Cache the datetime.datetime class and its `fromtimestamp`
     * classmethod for the lifetime of the module. */
    PyObject *datetime_module = PyImport_ImportModule("datetime");
    if (datetime_module == NULL) {
        Py_DECREF(m);
        return NULL;
    }
    _datetime_cls = PyObject_GetAttrString(datetime_module, "datetime");
    Py_DECREF(datetime_module);
    if (_datetime_cls == NULL) {
        Py_DECREF(m);
        return NULL;
    }
    _datetime_fromtimestamp = PyObject_GetAttrString(
        _datetime_cls, "fromtimestamp");
    if (_datetime_fromtimestamp == NULL) {
        Py_CLEAR(_datetime_cls);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
