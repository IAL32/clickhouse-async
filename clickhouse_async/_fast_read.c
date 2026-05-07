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
 */

#define Py_LIMITED_API 0x030B0000
#include <Python.h>

#define MODULE_NAME "_fast_read"
#define MODULE_VERSION "0.5.0-dev"

/*
 * Step 2 ships the scaffold only — a single no-op function that
 * confirms the module loaded and lets the codecs feature-detect the
 * fast path via a cheap call (or via attribute presence). The real
 * decode functions land in subsequent commits and replace this stub.
 */
static PyObject *
fast_read_available(PyObject *self, PyObject *args)
{
    Py_RETURN_TRUE;
}

static PyMethodDef FastReadMethods[] = {
    {"available", fast_read_available, METH_NOARGS,
     "Return True. Smoke test that the C extension was built and loaded."},
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
    return m;
}
