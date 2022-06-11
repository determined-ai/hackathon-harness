#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdbool.h>

#include "dctx.h"

typedef struct {
    PyObject_HEAD;
    bool open;
    dctx_t *dctx;
    int rank;
} py_dctx_t;

typedef struct {
    PyObject_HEAD;
    char *mem;
    Py_ssize_t len; // reusued as shape[0]
    Py_ssize_t stride; // always 1
    Py_ssize_t suboffset; // always -1
    // no refcount; we free mem when we are also freed
} py_cmem_t;

typedef struct {
    PyObject_HEAD;
    dc_op_t *op;
    bool extract;
    // the data we are operating on, which is not safe to free yet
    Py_buffer view;
} py_dc_op_t;

static PyTypeObject py_dctx_type;
static PyTypeObject py_cmem_type;
static PyTypeObject py_dc_op_type;

static py_cmem_t *cmem_new(char *mem, Py_ssize_t len){
    py_cmem_t *cmem = PyObject_New(py_cmem_t, &py_cmem_type);
    if(!cmem){
        free(mem);
        return NULL;
    }
    cmem->mem = mem;
    cmem->len = len;
    cmem->stride = 1;
    cmem->suboffset = -1;
    return cmem;
}


// main entrypoint for python module
PyObject* PyInit__pydctx(void);

static PyObject *pysm_error;  // XXX use a different error, maybe a builtin one

// DCTX

static void py_dctx_dealloc(py_dctx_t *self){
    if(self->open){
        self->open = false;
        dctx_close(&self->dctx);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int py_dctx_init(py_dctx_t *self, PyObject *args, PyObject *kwds){
    int rank;
    int size;
    int local_rank;
    int local_size;
    int cross_rank;
    int cross_size;
    const char *chief_host;
    const char *chief_svc;

    char *kwnames[] = {
        "rank",
        "size",
        "local_rank",
        "local_size",
        "cross_rank",
        "cross_size",
        "chief_host",
        "chief_svc",
        NULL,
    };

    int ret = PyArg_ParseTupleAndKeywords(
        args, kwds, "iiiiiiss", kwnames,
        &rank,
        &size,
        &local_rank,
        &local_size,
        &cross_rank,
        &cross_size,
        &chief_host,
        &chief_svc
    );
    if(!ret) return -1;

    ret = dctx_open(
        &self->dctx,
        rank,
        size,
        local_rank,
        local_size,
        cross_rank,
        cross_size,
        chief_host,
        chief_svc
    );
    if(ret){
        PyErr_SetString(pysm_error, "dctx_open() failed");
        return -1;
    }

    self->open = true;
    self->rank = rank;

    return 0;
}


static char * const py_dctx_close_doc =
    "close() -> None\n"
    "close networking, stop and join the background thread.";
static PyObject *py_dctx_close(py_dctx_t *self){
    if(self->open){
        self->open = false;
        dctx_close(&self->dctx);
    }
    Py_RETURN_NONE;
}

static PyObject *py_dctx_enter(py_dctx_t *self){
    Py_INCREF(self);
    return (PyObject*)self;
}

static PyObject *py_dctx_exit(py_dctx_t *self, PyObject *args){
    (void)args;
    return py_dctx_close(self);
}


static char * const py_dctx_gather_doc =
    "gather(data: bytes-like, series:str = '') -> Optional[List[bytes-like]]\n"
    "the chief returns data from all workers; workers return None.";
static PyObject *py_dctx_gather(
    py_dctx_t *self, PyObject *args, PyObject *kwds
){
    Py_buffer data = {0};
    const char *series = "";
    Py_ssize_t slen = 0;

    char *kwnames[] = {
        "data",
        "series",
        NULL,
    };

    int ret = PyArg_ParseTupleAndKeywords(
        args, kwds, "s*|s#", kwnames,
        &data,
        &series,
        &slen
    );
    if(!ret) return NULL;

    /* allocate an empty pyop so we can be confident we have a place to hold
       the data safe before kicking off the gather */
    py_dc_op_t *pyop = PyObject_New(py_dc_op_t, &py_dc_op_type);
    if(!pyop) goto fail_data;

    dc_op_t *op = dctx_gather_nofree(
        self->dctx, series, (size_t)slen, data.buf, (size_t)data.len
    );
    if(!dc_op_ok(op)){
        PyErr_SetString(pysm_error, "failed to create operation");
        goto fail_pyop;
    }

    // release the PyBuffer after awaiting the operation
    pyop->op = op;
    pyop->extract = false;
    pyop->view = data;

    return (PyObject*)pyop;

fail_pyop:
    Py_DECREF((PyObject*)pyop);
fail_data:
    PyBuffer_Release(&data);
    return NULL;
}


static char * const py_dctx_broadcast_doc =
    "broadcast(\n"
    "    data: Optional[bytes-like], series:str = ''\n"
    ") -> Optional[List[bytes-like]]\n"
    "All workers return what the chief submitted.  Worker data is ignored.";
static PyObject *py_dctx_broadcast(
    py_dctx_t *self, PyObject *args, PyObject *kwds
){
    Py_buffer data = {0};  // chief
    PyObject *ignore = NULL;  // non-chief
    const char *series = "";
    Py_ssize_t slen = 0;

    char *kwnames[] = {
        "data",
        "series",
        NULL,
    };

    int ret;
    if(self->rank == 0){
        // require a bytes-like object
        ret = PyArg_ParseTupleAndKeywords(
            args, kwds, "s*|s#", kwnames,
            &data,
            &series,
            &slen
        );
    }else{
        ret = PyArg_ParseTupleAndKeywords(
            args, kwds, "O|s#", kwnames,
            &ignore,
            &series,
            &slen
        );
    }
    if(!ret) return NULL;

    // preallocate the pyop so that we don't leak network ops on failure
    py_dc_op_t *pyop = PyObject_New(py_dc_op_t, &py_dc_op_type);
    if(!pyop) goto fail_data;

    dc_op_t *op = dctx_broadcast_copy(
        self->dctx, series, (size_t)slen, data.buf, (size_t)data.len
    );
    if(!dc_op_ok(op)){
        PyErr_SetString(pysm_error, "failed to create operation");
        goto fail_pyop;
    }

    // release the PyBuffer after awaiting the operation
    pyop->op = op;
    pyop->extract = true;
    if(self->rank == 0){
        PyBuffer_Release(&data);
    }

    return (PyObject*)pyop;

fail_pyop:
    Py_DECREF((PyObject*)pyop);
fail_data:
    PyBuffer_Release(&data);
    return NULL;
}


static char * const py_dctx_allgather_doc =
    "allgather(data: bytes-like, series:str = '') -> List[bytes-like]\n"
    "All workers return what each worker submitted.";
static PyObject *py_dctx_allgather(
    py_dctx_t *self, PyObject *args, PyObject *kwds
){
    Py_buffer data = {0};
    const char *series = "";
    Py_ssize_t slen = 0;

    char *kwnames[] = {
        "data",
        "series",
        NULL,
    };

    int ret = PyArg_ParseTupleAndKeywords(
        args, kwds, "s*|s#", kwnames,
        &data,
        &series,
        &slen
    );
    if(!ret) return NULL;

    /* allocate an empty pyop so we can be confident we have a place to hold
       the data safe before kicking off the gather */
    py_dc_op_t *pyop = PyObject_New(py_dc_op_t, &py_dc_op_type);
    if(!pyop) goto fail_data;

    dc_op_t *op = dctx_allgather_nofree(
        self->dctx, series, (size_t)slen, data.buf, (size_t)data.len
    );
    if(!dc_op_ok(op)){
        PyErr_SetString(pysm_error, "failed to create operation");
        goto fail_pyop;
    }

    // release the PyBuffer after awaiting the operation
    pyop->op = op;
    pyop->extract = false;
    pyop->view = data;

    return (PyObject*)pyop;

fail_pyop:
    Py_DECREF((PyObject*)pyop);
fail_data:
    PyBuffer_Release(&data);
    return NULL;
}


static PyMethodDef py_dctx_methods[] = {
    {
        .ml_name = "close",
        .ml_meth = (PyCFunction)(void*)py_dctx_close,
        .ml_flags = METH_NOARGS,
        .ml_doc = py_dctx_close_doc,
    },
    {
        .ml_name = "__enter__",
        .ml_meth = (PyCFunction)(void*)py_dctx_enter,
        .ml_flags = METH_NOARGS,
        .ml_doc = NULL,
    },
    {
        .ml_name = "__exit__",
        .ml_meth = (PyCFunction)(void*)py_dctx_exit,
        .ml_flags = METH_VARARGS,
        .ml_doc = NULL,
    },
    {
        .ml_name = "gather",
        .ml_meth = (PyCFunction)(void*)py_dctx_gather,
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = py_dctx_gather_doc,
    },
    {
        .ml_name = "broadcast",
        .ml_meth = (PyCFunction)(void*)py_dctx_broadcast,
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = py_dctx_broadcast_doc,
    },
    {
        .ml_name = "allgather",
        .ml_meth = (PyCFunction)(void*)py_dctx_allgather,
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = py_dctx_allgather_doc,
    },
    {NULL}, // sentinel
};


static PyTypeObject py_dctx_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    // this needs to be dotted to work with pickle and pydoc
    .tp_name = "_pydctx.DCTX",
    .tp_doc = "python bindings to dctx object",
    .tp_basicsize = sizeof(py_dctx_t),
    // 0 means "size is not variable"
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = (destructor) py_dctx_dealloc,
    .tp_methods = py_dctx_methods,
    .tp_init = (initproc)py_dctx_init,
};

// CMem type, implements buffer protocol and frees memory when no longer used

static void py_cmem_dealloc(py_cmem_t *self){
    if(self->mem) free(self->mem);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int py_cmem_init(py_cmem_t *self, PyObject *args, PyObject *kwds){
    (void)self;
    (void)args;
    (void)kwds;
    PyErr_SetString(pysm_error, "CMem can only be created in C code");
    return -1;
}

static int py_cmem_getbuffer(PyObject *exporter, Py_buffer *view, int flags){
    #define FAIL(msg) do { \
        PyErr_SetString(PyExc_BufferError, msg); \
        goto fail; \
    }while(0)

    py_cmem_t *cmem = (py_cmem_t*)exporter;
    if(flags & PyBUF_WRITABLE) FAIL("CMem refuses to make writable views");

    if(flags & PyBUF_FORMAT) view->format = "B";

    // view holds a reference to us
    Py_INCREF(exporter);

    *view = (Py_buffer){
        .buf = cmem->mem,
        .obj = exporter,
        .len = cmem->len,
        .itemsize = 1,
        .readonly = 1,
        .ndim = 1,
        .format = flags & PyBUF_FORMAT ? "B" : NULL,
        .shape = &cmem->len,
        .strides = &cmem->stride,
        .suboffsets = &cmem->suboffset,
        .internal = NULL,
    };

    #undef FAIL
    return 0;

fail:
    view->obj = NULL;
    return -1;
}

static PyBufferProcs py_cmem_bufferprocs = {
    .bf_getbuffer = py_cmem_getbuffer,
    // memory is freed when the exporter is freed
    .bf_releasebuffer = NULL,
};

static PyMethodDef py_cmem_methods[] = {
    {NULL}, // sentinel
};

static PyTypeObject py_cmem_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    // this needs to be dotted to work with pickle and pydoc
    .tp_name = "_pydctx.CMem",
    .tp_doc = "an distributed communcation operation that can be awaited",
    .tp_basicsize = sizeof(py_cmem_t),
    // 0 means "size is not variable"
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = (destructor) py_cmem_dealloc,
    .tp_methods = py_cmem_methods,
    .tp_init = (initproc)py_cmem_init,
    .tp_as_buffer = &py_cmem_bufferprocs,
};


// Operation type

static void py_dc_op_dealloc(py_dc_op_t *self){
    // release the buffer (PyBuffer_Release is safe against double-frees)
    PyBuffer_Release(&self->view);

    // TODO: need to separate dctx alloc/free from open/close to avoid errors
    // what a n00b error!

    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int py_dc_op_init(py_dc_op_t *self, PyObject *args, PyObject *kwds){
    (void)self;
    (void)args;
    (void)kwds;
    PyErr_SetString(pysm_error, "Operation can only be created in C code");
    return -1;
}


static PyObject *py_dc_op_wait(py_dc_op_t *self){
    // TODO: any way to support async behavior?
    dc_result_t *r = dc_op_await(self->op);
    if(!dc_result_ok(r)){
        PyErr_SetString(pysm_error, "operation failed");
        goto fail;
    }

    // done with view
    PyBuffer_Release(&self->view);

    size_t count = dc_result_count(r);
    // empty result case
    if(count == 0) Py_RETURN_NONE;

    // broadcast case (only ever one result)
    if(self->extract){
        char *data = dc_result_take(r, 0);
        Py_ssize_t len = (Py_ssize_t)dc_result_len(r, 0);
        py_cmem_t *cmem = cmem_new(data, len);
        if(!cmem) goto fail_result;
        return (PyObject*)cmem;
    }

    // multi result case
    PyObject *py_list = PyList_New((Py_ssize_t)count);
    if(!py_list) goto fail_result;

    for(size_t i = 0; i < (size_t)count; i++){
        char *data = dc_result_take(r, i);
        Py_ssize_t len = (Py_ssize_t)dc_result_len(r, i);

        // give data to a zero-copy, reference-counted wrapper object
        py_cmem_t *cmem = cmem_new(data, len);
        if(!cmem) goto fail_list;

        // the SET_ITEM macro is only suitable for newly created, empty lists
        // SET_ITEM steals a reference, so no decref is necessary
        PyList_SET_ITEM(py_list, (Py_ssize_t)i, (PyObject*)cmem);
    }

    return py_list;

fail_list:
    Py_DECREF(&py_list);
fail_result:
    dc_result_free2(r);
fail:
    return NULL;
}

static PyMethodDef py_dc_op_methods[] = {
    {
        .ml_name = "wait",
        .ml_meth = (PyCFunction)(void*)py_dc_op_wait,
        .ml_flags = METH_NOARGS,
        .ml_doc = NULL,
    },
    {NULL}, // sentinel
};


static PyTypeObject py_dc_op_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    // this needs to be dotted to work with pickle and pydoc
    .tp_name = "_pydctx.Operation",
    .tp_doc = "an distributed communcation operation that can be awaited",
    .tp_basicsize = sizeof(py_dc_op_t),
    // 0 means "size is not variable"
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = (destructor) py_dc_op_dealloc,
    .tp_methods = py_dc_op_methods,
    .tp_init = (initproc)py_dc_op_init,
};

////

static PyObject *junkfunc(PyObject *self){
    (void)self;
    const char *str = "hello world!";
    return (PyObject*)cmem_new(strdup(str), (Py_ssize_t)strlen(str));
}


#define ARG_KWARG_FN_CAST(fn)\
    (PyCFunction)(void(*)(void))(fn)

static PyMethodDef _pydctx_methods[] = {
    {
        .ml_name = "junkfunc",
        .ml_meth = ARG_KWARG_FN_CAST(junkfunc),
        .ml_flags = METH_NOARGS,
        .ml_doc = "do some junk",
    },
    /*
    {
        .ml_name = "log_to_file",
        .ml_meth = ARG_KWARG_FN_CAST(pysm_log_to_file),
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = pysm_log_to_file_doc,
    },
    {
        .ml_name = "to_fsid",
        .ml_meth = ARG_KWARG_FN_CAST(pysm_to_fsid),
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = "get an fsid for a uuid",
    },
    {
        .ml_name = "to_uuid",
        .ml_meth = ARG_KWARG_FN_CAST(pysm_to_uuid),
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = "get a uuid from an fsid",
    },
    {
        .ml_name = "valid_email",
        .ml_meth = ARG_KWARG_FN_CAST(pysm_valid_email),
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = "Raises a pysm.UserError if email is invalid.",
    },
    {
        .ml_name = "valid_password",
        .ml_meth = ARG_KWARG_FN_CAST(pysm_valid_password),
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = "Raises a pysm.UserError if password is invalid.",
    },
    */
    {0},  // sentinel
};

static struct PyModuleDef _pydctx_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_pydctx",
    .m_doc = "python bindings to dctx distributed communictions library",
    // XXX we don't have global state...?
    .m_size = -1, /* size of per-interpreter state of the module,
                     or -1 if the module keeps state in global variables. */
    .m_methods = _pydctx_methods,
};

PyObject* PyInit__pydctx(void){
    if (PyType_Ready(&py_dctx_type) < 0) return NULL;
    if (PyType_Ready(&py_cmem_type) < 0) return NULL;
    if (PyType_Ready(&py_dc_op_type) < 0) return NULL;
    int ret;

    PyObject *module = PyModule_Create(&_pydctx_module);
    if (module == NULL){
        return NULL;
    }

    Py_INCREF((PyObject*)&py_dctx_type);
    ret = PyModule_AddObject(module, "DCTX", (PyObject*)&py_dctx_type);
    if(ret < 0) goto fail_py_dctx;

    Py_INCREF((PyObject*)&py_cmem_type);
    ret = PyModule_AddObject(module, "CMem", (PyObject*)&py_cmem_type);
    if(ret < 0) goto fail_cmem;

    Py_INCREF((PyObject*)&py_dc_op_type);
    ret = PyModule_AddObject(module, "Operation", (PyObject*)&py_dc_op_type);
    if(ret < 0) goto fail_dc_op;

    pysm_error = PyErr_NewException("_pydctx.PysmError", NULL, NULL);
    Py_INCREF(pysm_error);
    ret = PyModule_AddObject(module, "PysmError", pysm_error);
    if(ret < 0) goto fail_pysm_error;

    return module;

fail_pysm_error:
    Py_DECREF((PyObject*)&pysm_error);
fail_dc_op:
    Py_DECREF((PyObject*)&py_dc_op_type);
fail_cmem:
    Py_DECREF((PyObject*)&py_cmem_type);
fail_py_dctx:
    Py_DECREF((PyObject*)&py_dctx_type);
    Py_DECREF(module);
    return NULL;
}
