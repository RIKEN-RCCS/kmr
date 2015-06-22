# kmr4py.py
# Copyright (C) 2012-2015 RIKEN AICS

"""Python Binding for KMR Map-Reduce Library.  This provides
straightforward wrappers to the C routines.  See more abort KMR at
"http://mt.aics.riken.jp/kmr".  All key-value data is stored in C
structures after encoding/decoding Python objects to byte arrays in C.
Documentation in Python is minimum, so please refer to the
documentation in C."""

## NOTE: Importing mpi4py initializes for MPI execution.  It is not
## imported here, applications shall import it.

import warnings
import ctypes
import cPickle
import inspect
import traceback
import sys

__version__ = "20150401"

kmrso = ctypes.CDLL("libkmr.so")

"""kmrso holds a libkmr.so library object."""

_kmrso_version = ctypes.c_int.in_dll(kmrso, "kmr_version").value
if (__version__ != str(_kmrso_version)):
    warnings.warn(("Version unmatch with libkmr.so;"
                   + " found=" + str(_kmrso_version)
                   + " required=" + __version__),
                  RuntimeWarning)

#cpickle_protocol = cPickle.HIGHEST_PROTOCOL
cpickle_protocol = 0

"""cpickle_protocol specifies a protocol used in encoding/decoding
key-value fields.  NOTE: The highest protocol of cpickle (value 2) is
avoided because it fails to encode/decode integer zero in
python-2.7.10, gcc-4.8.2, x86-64."""

warning_function = warnings.warn

"""warning_function specifies the function used to issue warnings."""

ignore_exceptions_in_map_fn = True

"""ignore_exceptions_in_map_fn=True makes exceptions ignored."""

print_backtrace_in_map_fn = True

"""print_backtrace_in_map_fn=True makes backtraces are printed at
exceptions in mapper/reducer functions."""

force_null_terminate_in_cstring = True

"""force_null_terminate_in_cstring specifies to add a null-terminator
in C strings.  Do not change it while some KVS'es are live."""

kmrso.kmr_init_2.argtypes = [ctypes.c_int]
kmrso.kmr_init_2.restype = ctypes.c_int

## Initializes KMR at this point.

kmrso.kmr_init_2(0)

_c_pointer = ctypes.POINTER(ctypes.c_char)
_c_kmr = _c_pointer
_c_kvs = _c_pointer
_c_kvsvec = ctypes.c_void_p
_c_boxvec = ctypes.c_void_p
_c_fnp = ctypes.c_void_p
_c_void_p = ctypes.c_void_p
_c_ubyte = ctypes.c_ubyte
_c_bool = ctypes.c_bool
_c_int = ctypes.c_int
_c_uint = ctypes.c_uint
_c_long = ctypes.c_long
_c_uint32 = ctypes.c_uint32
_c_uint64 = ctypes.c_uint64
_c_double = ctypes.c_double
_c_size_t = ctypes.c_size_t
_c_string = ctypes.c_char_p

## _c_funcptr is ctypes._FuncPtr, but it is taken indirectly
## because it is hidden.

_c_funcptr = type(kmrso.kmr_init_2)

## Null return values for ctypes.restype.

_c_null_pointer_value = _c_pointer()

def _c_null_pointer(p):
    """Returns true if ctypes pointer is null."""

    return (not bool(p))

def _string_of_options(o):
    """Returns a print string of options for _c_option,
    _c_file_option, and _c_spawn_option."""

    prefix = o.__class__.__name__
    attrs = o.__class__._fields_
    ss = []
    for (f, _, _) in attrs:
        if ((f not in ["gap16", "gap32"]) and getattr(o, f) == 1):
            ss.append(f + "=1")
    return (prefix + "(" + (",".join(ss)) + ")")

class _c_option(ctypes.Structure):
    """kmr_option."""

    _fields_ = [
        ("nothreading", _c_uint, 1),
        ("inspect", _c_uint, 1),
        ("keep_open", _c_uint, 1),
        ("key_as_rank", _c_uint, 1),
        ("rank_zero", _c_uint, 1),
        ("collapse", _c_uint, 1),
        ("take_ckpt", _c_uint, 1),
        ("gap16", _c_uint, 16),
        ("gap32", _c_uint, 32)]

    def __init__(self, opts=None, enabledlist=None):
        super(_c_option, self).__init__()
        if opts is None: opts = {}
        if enabledlist is None: enabledlist = []
        ## Sets the options as dictionary passed.
        for o, v in opts.iteritems():
            if (o in ["key", "value", "output"]):
                ## "key", "value", and "output" are Python binding only.
                pass
            elif (enabledlist != [] and (o not in enabledlist)):
                raise Exception("Bad option: %s" % o)
            elif (o == "nothreading"):
                self.nothreading = v
            elif (o == "inspect"):
                self.inspect = v
            elif (o == "keep_open"):
                self.keep_open = v
            elif (o == "key_as_rank"):
                self.key_as_rank = v
            elif (o == "rank_zero"):
                self.rank_zero = v
            elif (o == "collapse"):
                self.collapse = v
            elif (o == "take_ckpt"):
                self.take_ckpt = v
            else:
                raise Exception("Bad option: %s" % o)
        return

    def __str__(self):
        return _string_of_options(self)

class _c_file_option(ctypes.Structure):
    """kmr_file_option."""

    _fields_ = [
        ("each_rank", _c_uint, 1),
        ("subdirectories", _c_uint, 1),
        ("list_file", _c_uint, 1),
        ("shuffle_names", _c_uint, 1),
        ("gap16", _c_uint, 16),
        ("gap32", _c_uint, 32)]

    def __init__(self, opts=None, enabledlist=None):
        super(_c_file_option, self).__init__()
        if opts is None: opts = {}
        if enabledlist is None: enabledlist = []
        ## Sets the options as dictionary passed.
        for o, v in opts.iteritems():
            if (o == "key" or o == "output"):
                ## "key" and "output" are Python binding only.
                pass
            elif (enabledlist != [] and (o not in enabledlist)):
                raise Exception("Bad option: %s" % o)
            elif (o == "each_rank"):
                self.each_rank = v
            elif (o == "subdirectories"):
                self.subdirectories = v
            elif (o == "list_file"):
                self.list_file = v
            elif (o == "shuffle_names"):
                self.shuffle_names = v
            else:
                raise Exception("Bad option: %s" % o)
            return

    def __str__(self):
        return _string_of_options(self)

class _c_spawn_option(ctypes.Structure):
    """kmr_spawn_option."""

    _fields_ = [
        ("separator_space", _c_uint, 1),
        ("reply_each", _c_uint, 1),
        ("reply_root", _c_uint, 1),
        ("one_by_one", _c_uint, 1),
        ("gap16", _c_uint, 16),
        ("gap32", _c_uint, 32)]

    def __init__(self, opts=None, enabledlist=None):
        super(_c_spawn_option, self).__init__()
        if opts is None: opts = {}
        if enabledlist is None: enabledlist = []
        ## Sets the options as dictionary passed.
        for o, v in opts.iteritems():
            if (o == "key" or o == "output"):
                ## "key" and "output" are Python binding only.
                pass
            elif (enabledlist != [] and (o not in enabledlist)):
                raise Exception("Bad option: %s" % o)
            elif (o == "separator_space"):
                self.separator_space = v
            elif (o == "reply_each"):
                self.reply_each = v
            elif (o == "reply_root"):
                self.reply_root = v
            elif (o == "one_by_one"):
                self.one_by_one = v
            else:
                raise Exception("Bad option: %s" % o)
        return

    def __str__(self):
        return _string_of_options(self)

class _c_unitsized(ctypes.Union):
    """kmr_unit_sized {const char *p; long i; double d;}."""

    _fields_ = [
        ("p", _c_string),
        ("i", _c_long),
        ("d", _c_double)]

class _c_kvbox(ctypes.Structure):
    """kmr_kv_box {int klen, vlen; kmr_unit_sized k, v;}."""

    _fields_ = [
        ("klen", _c_int),
        ("vlen", _c_int),
        ("k", _c_unitsized),
        ("v", _c_unitsized)]

    ## NOTE: Defining __init__ with some arguments makes c-callback
    ## fail to call initializers.

    def __init__(self):
        super(_c_kvbox, self).__init__()

    def set(self, klen, key, vlen, val):
        self.klen = klen
        self.vlen = vlen
        self.k = key
        self.v = val
        return self

kmrso.kmr_mpi_type_size.argtypes = [_c_string]
kmrso.kmr_mpi_type_size.restype = _c_size_t

kmrso.kmr_mpi_constant_value.argtypes = [_c_string]
kmrso.kmr_mpi_constant_value.restype = _c_uint64

## Import some MPI constant values.  Calling kmr_mpi_type_size and
## kmr_mpi_constant_value dose not need MPI be initialized.

def _setup_mpi_constants():
    def c_type_by_size(siz):
        if (siz == ctypes.sizeof(_c_uint64)):
            return _c_uint64
        elif (siz == ctypes.sizeof(_c_uint32)):
            return _c_uint32
        else:
            raise Exception("Bad type size unknown: %d" % siz)
        return None
    global _c_mpi_comm, _c_mpi_info
    global _mpi_comm_world, _mpi_comm_self, _mpi_info_null
    siz = kmrso.kmr_mpi_type_size("MPI_Comm")
    _c_mpi_comm = c_type_by_size(siz)
    siz = kmrso.kmr_mpi_type_size("MPI_Info")
    _c_mpi_info = c_type_by_size(siz)
    _mpi_comm_world = kmrso.kmr_mpi_constant_value("MPI_COMM_WORLD")
    _mpi_comm_self = kmrso.kmr_mpi_constant_value("MPI_COMM_SELF")
    _mpi_info_null = kmrso.kmr_mpi_constant_value("MPI_INFO_NULL")
    return

_setup_mpi_constants()

kmrso.kmr_fin.argtypes = []
kmrso.kmr_fin.restype = _c_int

kmrso.kmr_create_context.argtypes = [_c_mpi_comm, _c_mpi_info, _c_string]
kmrso.kmr_create_context.restype = _c_pointer

kmrso.kmr_create_dummy_context.argtypes = []
kmrso.kmr_create_dummy_context.restype = _c_pointer

kmrso.kmr_free_context.argtypes = [_c_kmr]
kmrso.kmr_free_context.restype = None

kmrso.kmr_set_option_by_strings.argtypes = [_c_kmr, _c_string, _c_string]
kmrso.kmr_set_option_by_strings.restype = None

kmrso.kmr_create_kvs7.argtypes = [
    _c_kmr, _c_int, _c_int, _c_option, _c_string, _c_int, _c_string]
kmrso.kmr_create_kvs7.restype = _c_kvs

kmrso.kmr_add_kv.argtypes = [_c_kvs, _c_kvbox]
kmrso.kmr_add_kv.restype = None

kmrso.kmr_add_kv_done.argtypes = [_c_kvs]
kmrso.kmr_add_kv_done.restype = None

kmrso.kmr_get_element_count.argtypes = [_c_kvs]
kmrso.kmr_get_element_count.restype = _c_long

kmrso.kmr_local_element_count.argtypes = [_c_kvs]
kmrso.kmr_local_element_count.restype = _c_long

kmrso.kmr_map9.argtypes = [
    _c_bool, _c_kvs, _c_kvs, _c_void_p, _c_option, _c_fnp,
    _c_string, _c_int, _c_string]
kmrso.kmr_map9.restype = None

kmrso.kmr_map_once.argtypes = [_c_kvs, _c_void_p, _c_option, _c_bool, _c_fnp]
kmrso.kmr_map_once.restype = None

kmrso.kmr_map_rank_by_rank.argtypes = [
    _c_kvs, _c_kvs, _c_void_p, _c_option, _c_fnp]
kmrso.kmr_map_rank_by_rank.restype = None

kmrso.kmr_map_for_some.argtypes = [
    _c_kvs, _c_kvs, _c_void_p, _c_option, _c_fnp]
kmrso.kmr_map_for_some.restype = None

kmrso.kmr_map_ms.argtypes = [_c_kvs, _c_kvs, _c_void_p, _c_option, _c_fnp]
kmrso.kmr_map_ms.restype = _c_int

kmrso.kmr_map_ms_commands.argtypes = [
    _c_kvs, _c_kvs, _c_void_p, _c_option, _c_spawn_option, _c_fnp]
kmrso.kmr_map_ms_commands.restype = _c_int

kmrso.kmr_map_via_spawn.argtypes = [
    _c_kvs, _c_kvs, _c_void_p, _c_mpi_info, _c_spawn_option, _c_fnp]
kmrso.kmr_map_via_spawn.restype = None

kmrso.kmr_map_processes.argtypes = [
    _c_bool, _c_kvs, _c_kvs, _c_void_p, _c_mpi_info, _c_spawn_option, _c_fnp]
kmrso.kmr_map_processes.restype = None

kmrso.kmr_reduce9.argtypes = [
    _c_bool, _c_kvs, _c_kvs, _c_void_p, _c_option, _c_fnp,
    _c_string, _c_int, _c_string]
kmrso.kmr_reduce9.restype = None

kmrso.kmr_reduce_as_one.argtypes = [
    _c_kvs, _c_kvs, _c_void_p, _c_option, _c_fnp]
kmrso.kmr_reduce_as_one.type = None

kmrso.kmr_shuffle.argtypes = [_c_kvs, _c_kvs, _c_option]
kmrso.kmr_shuffle.restype = None

kmrso.kmr_replicate.argtypes = [_c_kvs, _c_kvs, _c_option]
kmrso.kmr_replicate.restype = None

kmrso.kmr_distribute.argtypes = [_c_kvs, _c_kvs, _c_bool, _c_option]
kmrso.kmr_distribute.restype = None

kmrso.kmr_concatenate_kvs.argtypes = [_c_kvsvec, _c_int, _c_kvs, _c_option]
kmrso.kmr_concatenate_kvs.restype = None

kmrso.kmr_reverse.argtypes = [_c_kvs, _c_kvs, _c_option]
kmrso.kmr_reverse.restype = None

kmrso.kmr_sort.argtypes = [_c_kvs, _c_kvs, _c_option]
kmrso.kmr_sort.restype = None

kmrso.kmr_sort_locally.argtypes = [_c_kvs, _c_kvs, _c_bool, _c_option]
kmrso.kmr_sort_locally.restype = None

kmrso.kmr_reply_to_spawner.argtypes = [_c_kmr]
kmrso.kmr_reply_to_spawner.restype = None

kmrso.kmr_send_kvs_to_spawner.argtypes = [_c_kmr, _c_kvs]
kmrso.kmr_send_kvs_to_spawner.restype = None

kmrso.kmr_get_spawner_communicator.argtypes = [_c_void_p, _c_long]
kmrso.kmr_get_spawner_communicator.restype = _c_mpi_comm

kmrso.kmr_read_files_reassemble.argtypes = [
    _c_kmr, _c_string, _c_int, _c_uint64, _c_uint64,
    ctypes.POINTER(_c_void_p), ctypes.POINTER(_c_uint64)]
kmrso.kmr_read_files_reassemble.restype = None

kmrso.kmr_read_file_by_segments.argtypes = [
    _c_kmr, _c_string, _c_int,
    ctypes.POINTER(_c_void_p), ctypes.POINTER(_c_uint64)]
kmrso.kmr_read_file_by_segments.restype = None

kmrso.kmr_save_kvs.argtypes = [
    _c_kvs, ctypes.POINTER(_c_void_p), ctypes.POINTER(_c_size_t), _c_option]
kmrso.kmr_save_kvs.restype = None

kmrso.kmr_restore_kvs.argtypes = [
    _c_kvs, _c_void_p, _c_size_t, _c_option]
kmrso.kmr_restore_kvs.restype = None

kmrso.kmr_dump_kvs.argtypes = [_c_kvs, _c_int]
kmrso.kmr_dump_kvs.restype = None

kmrso.kmr_get_key_type_ff.argtypes = [_c_kvs]
kmrso.kmr_get_key_type_ff.restype = _c_int

kmrso.kmr_get_value_type_ff.argtypes = [_c_kvs]
kmrso.kmr_get_value_type_ff.restype = _c_int

kmrso.kmr_get_nprocs.argtypes = [_c_kmr]
kmrso.kmr_get_nprocs.restype = _c_int

kmrso.kmr_get_rank.argtypes = [_c_kmr]
kmrso.kmr_get_rank.restype = _c_int

kmrso.kmr_mfree.argtypes = [_c_void_p, _c_size_t]
kmrso.kmr_mfree.restype = None

kmrso.kmr_stringify_options.argtypes = [_c_option]
kmrso.kmr_stringify_options.restype = _c_string

kmrso.kmr_stringify_file_options.argtypes = [_c_file_option]
kmrso.kmr_stringify_file_options.restype = _c_string

kmrso.kmr_stringify_spawn_options.argtypes = [_c_spawn_option]
kmrso.kmr_stringify_spawn_options.restype = _c_string

#receive_kvs_from_spawned_fn = kmrso.kmr_receive_kvs_from_spawned_fn

_kv_bad = ctypes.c_int.in_dll(kmrso, "kmr_kv_field_bad").value
_kv_opaque = ctypes.c_int.in_dll(kmrso, "kmr_kv_field_opaque").value
_kv_cstring = ctypes.c_int.in_dll(kmrso, "kmr_kv_field_cstring").value
_kv_integer = ctypes.c_int.in_dll(kmrso, "kmr_kv_field_integer").value
_kv_float8 = ctypes.c_int.in_dll(kmrso, "kmr_kv_field_float8").value

_spawn_option_list = [_k for (_k, _, _) in _c_spawn_option._fields_]
_file_option_list = [_k for (_k, _, _) in _c_file_option._fields_]

_field_name_type_map = {
    "opaque" : _kv_opaque, "cstring" : _kv_cstring,
    "integer" : _kv_integer, "float8" : _kv_float8}

_field_type_name_map = dict(
    map(tuple, map(reversed, _field_name_type_map.items())))

## C-callable function factories.

_MKMAPFN = ctypes.CFUNCTYPE(_c_int, _c_kvbox, _c_kvs, _c_kvs,
                            _c_void_p, _c_long)
_MKREDFN = ctypes.CFUNCTYPE(_c_int, _c_boxvec, _c_long,
                            _c_kvs, _c_kvs, _c_void_p)

## Argtypes of C callback map/reduce functions.

_c_mapfn_argtypes = [_c_kvbox, _c_kvs, _c_kvs, _c_void_p, _c_long]
_c_mapfn_restype = _c_int

_c_redfn_argtypes = [_c_boxvec, _c_long, _c_kvs, _c_kvs, _c_void_p]
_c_redfn_restype = _c_int

def _wrap_mapfn(pyfn):
    """Returns a closure which calls a given Python map-function on
    the unmarshalled contents in KVS."""

    if (pyfn is None):
        return 0
    elif (isinstance(pyfn, _c_funcptr)):
        return pyfn
    else:
        def applyfn(cbox, ckvi, ckvo, carg, cindex):
            kvi = KVS(ckvi)
            kvo = KVS(ckvo)
            key = kvi._decode_content(cbox.klen, cbox.k, "key")
            val = kvi._decode_content(cbox.vlen, cbox.v, "value")
            try:
                pyfn((key, val), kvi, kvo, cindex)
            except:
                warning_function(("Exception in Python callbacks: %s"
                                  % str(sys.exc_info()[1])),
                                 RuntimeWarning)
                if (print_backtrace_in_map_fn): traceback.print_exc()
            return (0 if ignore_exceptions_in_map_fn else -1)
        return _MKMAPFN(applyfn)

def _wrap_redfn(pyfn):
    """Returns a closure which calls a given Python reduce-function on
    the unmarshalled contents in KVS."""

    if (pyfn is None):
        return 0
    elif (isinstance(pyfn, _c_funcptr)):
        return pyfn
    else:
        def applyfn(cboxvec, n, ckvi, ckvo, carg):
            kvi = KVS(ckvi)
            kvo = KVS(ckvo)
            kvvec = []
            for i in range(0, n):
                pos = (cboxvec + ctypes.sizeof(_c_kvbox) * i)
                cbox = _c_kvbox.from_address(pos)
                key = kvi._decode_content(cbox.klen, cbox.k, "key")
                val = kvi._decode_content(cbox.vlen, cbox.v, "value")
                kvvec.append((key, val))
            try:
                pyfn(kvvec, kvi, kvo)
            except:
                warning_function(("Exception in Python callbacks: %s"
                                  % str(sys.exc_info()[1])),
                                 RuntimeWarning)
                if (print_backtrace_in_map_fn): traceback.print_exc()
            return (0 if ignore_exceptions_in_map_fn else -1)
        return _MKREDFN(applyfn)

def _get_options(opts, with_keyty_valty):
    """Returns a triple of the options: a key field type, a value
    field type, and a flag of needs of output generation."""

    if ((not with_keyty_valty) and (("key" in opts) or ("value" in opts))):
        raise Exception("Bad option: key= or value= not allowed")
    keyty = opts.get("key", "opaque")
    valty = opts.get("value", "opaque")
    mkkvo = opts.get("output", True)
    return (keyty, valty, mkkvo)

def _make_frame_info(frame):
    sp = frame
    co = sp.f_code
    return (co.co_filename, sp.f_lineno, co.co_name)

def _filter_spawn_options(opts):
    """Returns a pair of dictionaries, the 1st holds options to spawn,
    and the 2nd holds the other options."""

    sopts = dict()
    mopts = dict()
    for o, v in opts.iteritems():
        if (o in _spawn_option_list):
            sopts[o] = v
        else:
            mopts[o] = v
    return (sopts, mopts)

class KMR():
    """KMR context."""

    ## attributes: self._ckmr, self.nprocs, self.rank, self.emptykvs
    ## self._dismissed

    def __init__(self, comm, info=None):
        """Makes a KMR context with a given MPI communicator (comm),
        which is used in succeeding operations.  Info specifies its
        options by MPI_Info.  Arguments of comm/info are passed as a
        long integer (assuming either an integer (int) or a pointer in
        C).  It also accepts a string "dummy" or "world" as a comm
        argument."""

        if (isinstance(info, (int, long))):
            warninfo = False
            cinfo = info
        else:
            warninfo = (info != None)
            cinfo = _mpi_info_null
        if (isinstance(comm, (int, long))):
            warncomm = False
            ccomm = comm
        elif (comm == "dummy"):
            warncomm = False
            ccomm = _mpi_comm_self
        elif (comm == "world"):
            warncomm = False
            ccomm = _mpi_comm_world
        else:
            warncomm = True
            ccomm = _mpi_comm_world

        self._ckmr = kmrso.kmr_create_context(ccomm, cinfo, "")

        """self._ckmr holds the C part of a KMR context."""

        if (_c_null_pointer(self._ckmr)):
            raise Exception("kmr_create_context: failed")

        self._dismissed = False

        """self._dismissed=True disables freeing KVS'es (by memory
        management) which remain unconsumed after dismissing a KMR
        context.  It is because freeing them causes referencing
        dangling pointers in C."""

        self.emptykvs = KVS(self).free()

        """self.emptykvs holds an empty KVS needed by map_once,
        map_on_rank_zero, read_files_reassemble, and
        read_file_by_segments."""

        self.nprocs = kmrso.kmr_get_nprocs(self._ckmr)

        """self.nprocs holds an nprocs of MPI."""

        self.rank = kmrso.kmr_get_rank(self._ckmr)

        """self.rank holds a rank of MPI."""

        if (warncomm and (self.rank == 0)):
            warning_function("MPI comm ignored in KMR() constructor.", RuntimeWarning)
        if (warninfo and (self.rank == 0)):
            warning_function("MPI info ignored in KMR() constructor.", RuntimeWarning)
        return

    def __del__(self):
        self.dismiss()
        return

    def free(self):
        """Dismisses KMR (an alias of dismiss())."""

        self.dismiss()

    def dismiss(self):
        """Dismisses KMR."""

        if (not _c_null_pointer(self._ckmr)):
            kmrso.kmr_free_context(self._ckmr)
        self._ckmr = _c_null_pointer_value
        self._dismissed = True
        self.emptykvs = None
        self.nprocs = -1
        self.rank = -1
        return

    def create_kvs(self, **opts):
        """Makes a new KVS (an alias of make_kvs())."""

        self.make_kvs(**opts)

    def make_kvs(self, **opts):
        """Makes a new KVS."""

        (keyty, valty, _) = _get_options(opts, True)
        return KVS(self, keyty, valty)

    def reply_to_spawner(self):
        """Sends a reply message from a spawned process."""

        kmrso.kmr_reply_to_spawner(self._ckmr)
        return

    def get_spawner_communicator(self, index):
        """Obtains a parent communicator of a spawned process."""

        return kmrso.kmr_get_spawner_communicator(self._ckmr, index)

    def send_kvs_to_spawner(self, kvs):
        """Sends the KVS from a spawned process to the spawner."""

        return kmrso.kmr_send_kvs_to_spawner(self._ckmr, kvs._ckvs)

    def set_option(self, k, v):
        """Sets KMR option, taking both arguments by strings."""

        kmrso.kmr_set_option_by_strings(self._ckmr, k, v)
        return

_enabled_options_of_map = [
    "nothreading", "inspect", "keep_open", "take_ckpt"]

_enabled_options_of_map_once = [
    "nothreading", "keep_open", "take_ckpt"]

_enabled_options_of_map_ms = [
    "nothreading", "keep_open"]

_enabled_options_of_reduce = [
    "nothreading", "inspect", "take_ckpt"]

_enabled_options_of_reduce_as_one = [
    "inspect", "take_ckpt"]

_enabled_options_of_shuffle = [
    "inspect", "rank_zero", "take_ckpt"]

_enabled_options_of_distribute = [
    "nothreading", "inspect", "keep_open"]

_enabled_options_of_sort_locally = [
    "nothreading", "inspect", "key_as_rank"]

_enabled_options_of_sort = [
    "inspect"]

class KVS():
    """KVS.  Note that there are dummy KVS'es which are temporarily
    created to hold the C structure of the KVS passed to
    mapper/reducer functions.  A dummy KVS has None in its "mr"
    attribute."""

    ## attributes: self._ckvs, self.mr

    def __init__(self, kmr_or_ckvs, keyty="opaque", valty="opaque"):
        """Makes a KVS for a given KMR.  A KVS is created by
        specifying the datatypes stored in the key and the value,
        using the keywords "key=" and "value=".  The datatype name is
        a string, one of "opaque", "cstring", "integer", and "float8".
        Thus, most mappers and reducers (precisely, the ones which
        accepts a function argument) take keyword arguments with the
        defaults key="opaque" and value="opaque".  The datatypes
        affects the sorting order.  Do not call constructors directly,
        but via KMR.make_kvs()."""

        self.mr = None

        """mr attribute holds a KMR context object.  Note that mr is
        not accessible from mapping/reducing functions."""

        if isinstance(kmr_or_ckvs, KMR):
            kf = _field_name_type_map[keyty]
            vf = _field_name_type_map[valty]
            top = inspect.currentframe().f_back
            self.mr = kmr_or_ckvs
            (f, l, n) = _make_frame_info(top)
            self._ckvs = kmrso.kmr_create_kvs7(
                self.mr._ckmr, kf, vf, _c_option(), f, l, n)
        elif isinstance(kmr_or_ckvs, _c_pointer):
            ## Return a dummy KVS.
            self.mr = None
            self._ckvs = kmr_or_ckvs
        else:
            raise Exception("Bad call to kvs constructor")

    def __del__(self):
        if ((not self._is_dummy()) and (not _c_null_pointer(self._ckvs))):
            self.free()
        return

    def free(self):
        """Finishes the C part of a KVS."""

        if (self._is_dummy()):
            raise Exception("Bad call to free_kvs on dummy KVS")
        elif (_c_null_pointer(self._ckvs)):
            raise Exception("Bad call to free_kvs on freed KVS")
        elif ((not self.mr is None) and self.mr._dismissed):
            ## Do not free when KMR object is dismissed.
            pass
        else:
            kmrso.kmr_free_kvs(self._ckvs)
            self._ckvs = _c_null_pointer_value
            return self

    def _is_dummy(self):
        return (self.mr is None)

    def _consume(self):
        """Releases a now dangling C pointer."""

        self._ckvs = _c_null_pointer_value

    def _encode_content(self, o, key_or_value):
        """Marshalls an object with regard to the field type.  It
        retuns a 3-tuple, with length, value-union, and the 3nd to
        keep a reference to a buffer."""

        kvty = self.get_field_type(key_or_value)
        u = _c_unitsized()
        if (kvty == "opaque"):
            data = cPickle.dumps(o, cpickle_protocol)
            u.p = data
            return (len(data), u, data)
        elif (kvty == "cstring"):
            if (not isinstance(o, str)):
                raise Exception("Not 8-bit string for cstring: %s" % o)
            ## (Add null for C string).
            data = ((o + "\0") if force_null_terminate_in_cstring else o)
            u.p = data
            return (len(data), u, data)
        elif (kvty == "integer"):
            u.i = o
            return (ctypes.sizeof(_c_long), u, None)
        elif (kvty == "float8"):
            u.d = o
            return (ctypes.sizeof(_c_double), u, None)
        else:
            raise Exception("Bad field type: %s" % kvty)

    def _decode_content(self, siz, u, key_or_value):
        """Unmarshalls an object with regard to the field type.  It
        returns integer 0 when the length is 0 (it is for a dummy
        key-value used in kmr_map_once() etc)."""

        if (siz == 0):
            return 0
        else:
            kvty = self.get_field_type(key_or_value)
            if (kvty == "opaque"):
                s = ctypes.string_at(u.p, siz)
                o = cPickle.loads(s)
                return o
            elif (kvty == "cstring"):
                ## (Delete null added for C string).
                siz1 = ((siz - 1) if force_null_terminate_in_cstring else siz)
                s = ctypes.string_at(u.p, siz1)
                return s
            elif (kvty == "integer"):
                return u.i
            elif (kvty == "float8"):
                return u.d
            else:
                raise Exception("Bad field type: %s" % kvty)

    def get_field_type(self, key_or_value):
        """Get a field type of a KVS."""

        if (_c_null_pointer(self._ckvs)):
            raise Exception("Bad KVS (null C-object)")
        if (key_or_value == "key"):
            kvty = kmrso.kmr_get_key_type_ff(self._ckvs)
        elif (key_or_value == "value"):
            kvty = kmrso.kmr_get_value_type_ff(self._ckvs)
        else:
            raise Exception("Bad field %s" % key_or_value)
        if (kvty == _kv_bad):
            raise Exception("Bad field type value %d in KVS" % kvty)
        else:
            return _field_type_name_map[kvty]

    def add(self, key, val):
        """Adds a key-value pair."""

        self.add_kv(key, val)
        return

    def add_kv(self, key, val):
        """Adds a key-value pair."""

        ## Note it keeps the created string until kmr_add_kv(),
        ## because kvbox does not hold the references.
        (klen, k, ks) = self._encode_content(key, "key")
        (vlen, v, vs) = self._encode_content(val, "value")
        cbox = _c_kvbox().set(klen, k, vlen, v)
        kmrso.kmr_add_kv(self._ckvs, cbox)
        return

    def add_kv_done(self):
        """Finishes adding key-value pairs."""

        kmrso.kmr_add_kv_done(self._ckvs)
        return

    def get_element_count(self):
        """Gets the total number of key-value pairs."""

        c = _c_long(0)
        kmrso.kmr_get_element_count(self._ckvs, ctypes.byref(c))
        return c.value

    def local_element_count(self):
        """Gets the number of key-value pairs locally."""

        c = _c_long(0)
        kmrso.kmr_local_element_count(self._ckvs, ctypes.byref(c))
        return c.value

    def map(self, fn, **mopts):
        """Maps simply."""

        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_map)
        cfn = _wrap_mapfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        (f, l, n) = _make_frame_info(inspect.currentframe().f_back)
        kmrso.kmr_map9(0, ckvi, ckvo, 0, cmopts, cfn, *(f, l, n))
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def map_once(self, rank_zero_only, fn, **mopts):
        """Maps once with a dummy key-value pair."""

        ## It needs dummy input; Never inspects.
        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_map_once)
        cfn = _wrap_mapfn(fn)
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_map_once(ckvo, 0, cmopts, rank_zero_only, cfn)
        return kvo

    def map_on_rank_zero(self, fn, **mopts):
        """Maps on rank0 only."""

        ## It needs dummy input.
        return self.map_once(True, fn, *mopts)

    def map_rank_by_rank(self, fn, **mopts):
        """Maps sequentially with rank by rank for debugging."""

        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_map)
        cfn = _wrap_mapfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_map_rank_by_rank(ckvi, ckvo, 0, cmopts, cfn)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def map_for_some(self, fn, **mopts):
        """Maps until some key-value are added."""

        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_map)
        ckvi = self._ckvs
        cfn = _wrap_mapfn(fn)
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_map_for_some(ckvi, ckvo, 0, cmopts, cfn)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def map_ms(self, fn, **mopts):
        """Maps in master-slave mode."""

        ## Its call is repeated until True (assuming MPI_SUCCESS==0).
        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_map_ms)
        cfn = _wrap_mapfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        rr = 1
        while (rr != 0):
            rr = kmrso.kmr_map_ms(ckvi, ckvo, 0, cmopts, cfn)
        self._consume()
        return kvo

    def map_ms_commands(self, fn, **xopts):
        """Maps in master-slave mode, and runs serial commands."""

        (sopts, mopts) = _filter_spawn_options(xopts)
        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_map_ms)
        csopts = _c_spawn_option(sopts)
        cfn = _wrap_mapfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        rr = 1
        while (rr != 0):
            rr = kmrso.kmr_map_ms_commands(ckvi, ckvo, 0, cmopts, csopts, cfn)
        self._consume()
        return kvo

    def map_via_spawn(self, fn, **xopts):
        """Maps on processes started by MPI_Comm_spawn()."""

        (sopts, mopts) = _filter_spawn_options(xopts)
        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_map)
        csopts = _c_spawn_option(sopts)
        cfn = _wrap_mapfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_map_via_spawn(ckvi, ckvo, 0, _mpi_info_null, csopts, cfn)
        self._consume()
        return kvo

    def map_processes(self, nonmpi, fn, **sopts):
        """Maps on processes started by MPI_Comm_spawn()."""

        (keyty, valty, mkkvo) = _get_options(sopts, True)
        csopts = _c_spawn_option(sopts)
        cfn = _wrap_mapfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_map_processes(nonmpi, ckvi, ckvo, 0, _mpi_info_null,
                                csopts, cfn)
        self._consume()
        return kvo

    def map_parallel_processes(self, fn, **sopts):
        """Maps on processes started by MPI_Comm_spawn()."""

        return self.map_processes(False, fn, **sopts)

    def map_serial_processes(self, fn, **sopts):
        """Maps on processes started by MPI_Comm_spawn()."""

        return self.map_processes(True, fn, **sopts)

    def reduce(self, fn, **mopts):
        """Reduces key-value pairs."""

        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_reduce)
        cfn = _wrap_redfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        (f, l, n) = _make_frame_info(inspect.currentframe().f_back)
        kmrso.kmr_reduce9(0, ckvi, ckvo, 0, cmopts, cfn, *(f, l, n))
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def reduce_as_one(self, fn, **mopts):
        """ Reduces once as if all pairs had the same key."""

        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_reduce_as_one)
        cfn = _wrap_redfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_reduce_as_one(ckvi, ckvo, 0, cmopts, cfn)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def reduce_for_some(self, fn, **mopts):
        """Reduces until some key-value are added."""

        (keyty, valty, mkkvo) = _get_options(mopts, True)
        cmopts = _c_option(mopts, _enabled_options_of_reduce)
        cfn = _wrap_redfn(fn)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        ## (NOTE: It passes a frame of reduce_for_some.)
        (f, l, n) = _make_frame_info(inspect.currentframe())
        kmrso.kmr_reduce9(1, ckvi, ckvo, 0, cmopts, cfn, *(f, l, n))
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def reverse(self, **mopts):
        """Makes a new pair by swapping the key and the value."""

        keyty = self.get_field_type("key")
        valty = self.get_field_type("value")
        (_, _, mkkvo) = _get_options(mopts, False)
        cmopts = _c_option(mopts, _enabled_options_of_map)
        assert (mkkvo is True)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, valty, keyty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_reverse(ckvi, ckvo, cmopts)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def shuffle(self, **mopts):
        """Shuffles key-value pairs."""

        keyty = self.get_field_type("key")
        valty = self.get_field_type("value")
        (_, _, mkkvo) = _get_options(mopts, False)
        cmopts = _c_option(mopts, _enabled_options_of_reduce)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_shuffle(ckvi, ckvo, cmopts)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def replicate(self, **mopts):
        """Replicates key-value pairs to be visible on all ranks."""

        keyty = self.get_field_type("key")
        valty = self.get_field_type("value")
        (_, _, mkkvo) = _get_options(mopts, False)
        cmopts = _c_option(mopts, _enabled_options_of_shuffle)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_replicate(ckvi, ckvo, cmopts)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def distribute(self, cyclic, **mopts):
        """Distributes pairs approximately evenly to ranks."""

        keyty = self.get_field_type("key")
        valty = self.get_field_type("value")
        (_, _, mkkvo) = _get_options(mopts, False)
        cmopts = _c_option(mopts, _enabled_options_of_distribute)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_distribute(ckvi, ckvo, cyclic, cmopts)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def sort_locally(self, shuffling, **mopts):
        """Reorders key-value pairs in a single rank."""

        keyty = self.get_field_type("key")
        valty = self.get_field_type("value")
        (_, _, mkkvo) = _get_options(mopts, False)
        cmopts = _c_option(mopts, _enabled_options_of_sort_locally)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_sort_locally(ckvi, ckvo, shuffling, cmopts)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def sort(self, **mopts):
        """Sorts a KVS globally."""

        keyty = self.get_field_type("key")
        valty = self.get_field_type("value")
        (_, _, mkkvo) = _get_options(mopts, False)
        cmopts = _c_option(mopts, _enabled_options_of_sort)
        ckvi = self._ckvs
        kvo = (KVS(self.mr, keyty, valty) if mkkvo else None)
        ckvo = (kvo._ckvs if (kvo is not None) else None)
        kmrso.kmr_sort(ckvi, ckvo, cmopts)
        if (cmopts.inspect == 0): self._consume()
        return kvo

    def concatenate(self, *morekvs):
        """Concatenates a number of KVS'es to one."""

        keyty = self.get_field_type("key")
        valty = self.get_field_type("value")
        siz = (len(morekvs) + 1)
        ckvsvec = (_c_kvs * siz)()
        ckvsvec[0] = self._ckvs
        for i in range(0, len(morekvs)):
            ckvsvec[i + 1] = morekvs[i]._ckvs
        cn = _c_int(siz)
        kvo = KVS(self.mr, keyty, valty)
        ckvo = kvo._ckvs
        kmrso.kmr_concatenate_kvs(ckvsvec, cn, ckvo, _c_option())
        for i in morekvs:
            i._consume()
        self._consume()
        return kvo

    def read_files_reassemble(self, filename, color, offset, bytes_):
        """Reassembles files reading by ranks."""

        buf = _c_void_p()
        siz = _c_uint64(0)
        kmrso.kmr_read_files_reassemble(
            self.mr._ckmr, filename, color, offset, bytes_,
            ctypes.byref(buf), ctypes.byref(siz))
        addr = buf.value
        ptr = (_c_ubyte * siz.value).from_address(addr)
        data = bytearray(ptr)
        kmrso.kmr_mfree(addr, siz.value)
        return data

    def read_file_by_segments(self, filename, color):
        """Reads one file by segments and reassembles."""

        buf = _c_void_p()
        siz = _c_uint64(0)
        kmrso.kmr_read_file_by_segments(
            self.mr._ckmr, filename, color,
            ctypes.byref(buf), ctypes.byref(siz))
        addr = buf.value
        ptr = (_c_ubyte * siz.value).from_address(addr)
        data = bytearray(ptr)
        kmrso.kmr_mfree(addr, siz.value)
        return data

    def save(self):
        """Packs locally the contents of a KVS to a byte array."""

        buf = _c_void_p(0)
        siz = _c_size_t(0)
        kmrso.kmr_save_kvs(self._ckvs, ctypes.byref(buf), ctypes.byref(siz),
                           _c_option())
        addr = buf.value
        ptr = (_c_ubyte * siz.value).from_address(addr)
        data = bytearray(ptr)
        kmrso.kmr_mfree(addr, siz.value)
        return data

    def restore(self, data):
        """Unpacks locally the contents of a KVS from a byte array."""

        kvo = KVS(self.mr, "opaque", "opaque")
        siz = len(data)
        addr = (_c_ubyte * siz).from_buffer(data)
        kmrso.kmr_restore_kvs(kvo._ckvs, addr, siz, _c_option())
        return kvo

def fin():
    """Finishes using KMR4PY."""

    kmrso.kmr_fin()
    return

def listify(kvs):
    """Returns an array of LOCAL contents."""

    a = kvs.local_element_count() * [None]
    def f (kv, kvi, kvo, i, *_data):
        a[i] = kv
        return 0
    kvo = kvs.map(f, output=False, inspect=True)
    assert (kvo is None)
    return a

def _check_ctypes_values():
    """Checks if ctypes values are properly used."""

    if (not _c_null_pointer(_c_null_pointer_value)):
        raise Exception("BAD: C null pointer has a wrong value.")

def _check_passing_options():
    """Checks if the options are passed properly from Python to C."""

    for (option, stringify) in [
        (_c_option, kmrso.kmr_stringify_options),
        (_c_file_option, kmrso.kmr_stringify_file_options),
        (_c_spawn_option, kmrso.kmr_stringify_spawn_options)]:
        for (o, _, _) in option._fields_:
            if ((o == "gap16") or (o == "gap32")):
                pass
            else:
                copts = option({o : 1})
                s = stringify(copts)
                if (o != s):
                    raise Exception("BAD: %s != %s" % (str(o), str(s)))

# Copyright (C) 2012-2015 RIKEN AICS
# This library is distributed WITHOUT ANY WARRANTY.  This library can be
# redistributed and/or modified under the terms of the BSD 2-Clause License.
