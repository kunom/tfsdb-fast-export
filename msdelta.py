"""Wrapper module to access the MSDelta deltification library."""

import ctypes
import ctypes.wintypes


# Type definitions
# ----------------

LPBUFFER = ctypes.POINTER(ctypes.c_char) # is not 0-terminated, in contrast to 'ctypes.c_char_p' (see ctypes documentation)

DELTA_FLAG_TYPE = ctypes.c_ulonglong

class DELTA_INPUT(ctypes.Structure):
    _fields_ = [
        ("lpStart", LPBUFFER),
        ("uSize", ctypes.wintypes.ULONG),
        ("Editable", ctypes.wintypes.BOOL)]

class DELTA_OUTPUT(ctypes.Structure):
    _fields_ = [
        ("lpStart", LPBUFFER),
        ("uSize", ctypes.wintypes.ULONG)]

DELTA_FILE_TYPE_RAW = 1

DELTA_FILE_TYPE_SET_RAW_ONLY = DELTA_FILE_TYPE_RAW

DELTA_FLAG_NONE = 0
DELTA_APPLY_FLAG_ALLOW_PA19 = 1


# Native interface
# ----------------

def _winerror_on_failure(result, func, arguments):
    if not result:
        raise ctypes.WinError()
    return result

_dll = ctypes.windll.msdelta

_createDeltaB = _dll.CreateDeltaB
_createDeltaB.argtypes = [DELTA_FLAG_TYPE, DELTA_FLAG_TYPE, DELTA_FLAG_TYPE, DELTA_INPUT, DELTA_INPUT, DELTA_INPUT, DELTA_INPUT, DELTA_INPUT, ctypes.wintypes.LPFILETIME, ctypes.wintypes.UINT, ctypes.POINTER(DELTA_OUTPUT)]
_createDeltaB.errcheck = _winerror_on_failure

_applyDeltaB = _dll.ApplyDeltaB
_applyDeltaB.argtypes = [DELTA_FLAG_TYPE, DELTA_INPUT, DELTA_INPUT, ctypes.POINTER(DELTA_OUTPUT)]
_applyDeltaB.errcheck = _winerror_on_failure

_applyDeltaW = _dll.ApplyDeltaW
_applyDeltaW.argtypes = [DELTA_FLAG_TYPE, ctypes.wintypes.LPCWSTR, ctypes.wintypes.LPCWSTR, ctypes.wintypes.LPCWSTR]
_applyDeltaW.errcheck = _winerror_on_failure

_deltaFree = _dll.DeltaFree
_deltaFree.argtypes = [LPBUFFER]
_deltaFree.errcheck = _winerror_on_failure


# Public interface
# ----------------

MagicNumber = b'PA19'

def CreateDeltaB(source, target, fileTypeSet = DELTA_FILE_TYPE_SET_RAW_ONLY, set_flags = DELTA_FLAG_NONE, reset_flags = DELTA_FLAG_NONE):
    """Creates a delta based on in-memory buffers."""
    
    # TODO: not sure about how many copies acutally are done in memory...

    diSource = DELTA_INPUT(ctypes.create_string_buffer(source), len(source), False)
    diTarget = DELTA_INPUT(ctypes.create_string_buffer(target), len(target), False)
    diEmpty = DELTA_INPUT(None, 0, False)
    doResult = DELTA_OUTPUT()

    _createDeltaB(
        fileTypeSet, set_flags, reset_flags, 
        diSource, diTarget, diEmpty, diEmpty, diEmpty, 
        None, 
        0,
        ctypes.byref(doResult))

    result = doResult.lpStart[:doResult.uSize]
    _deltaFree(doResult.lpStart)

    return result

def ApplyDeltaB(source, delta, flags = DELTA_APPLY_FLAG_ALLOW_PA19):
    """Applies a delta based on in-memory buffers."""

    # TODO: not sure about how many copies are actually being created...

    diSource = DELTA_INPUT(ctypes.create_string_buffer(source), len(source), False)
    diDelta = DELTA_INPUT(ctypes.create_string_buffer(delta), len(delta), False)    
    doResult = DELTA_OUTPUT()

    _applyDeltaB(flags, diSource, diDelta, ctypes.byref(doResult))

    result = doResult.lpStart[:doResult.uSize]
    _deltaFree(doResult.lpStart)

    return result

def ApplyDelta(sourcePath, deltaPath, targetPath, flags = DELTA_APPLY_FLAG_ALLOW_PA19):
    """Applies a delta based on file content."""

    _applyDeltaW(flags, sourcePath, deltaPath, targetPath)