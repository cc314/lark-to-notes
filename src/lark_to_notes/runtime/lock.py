"""Advisory file lock for inter-process serialization of vault writes.

The :class:`RuntimeLock` wraps POSIX ``fcntl.flock`` (exclusive, non-blocking
try-acquire followed by a blocking acquire) to provide a portable inter-process
mutex.  A single context-manager usage pattern keeps call sites clean::

    lock = RuntimeLock(lock_path=vault_root / ".ltn.lock")
    with lock:
        writer.render_pipeline(item)

The lock file itself is created on first use and never deleted (RULE 1).

Design notes
------------
* Uses blocking ``LOCK_EX`` so callers are serialized; there is no timeout at
  this level.  Long-running callers should run the lock in a background thread
  if they need cancellation.
* The lock-holder PID is written into the file for diagnostics.
* On macOS and Linux, ``flock`` is per-open-file-description, so the lock is
  released automatically when the file descriptor is closed (i.e., on
  ``__exit__`` or process exit), even if the process crashes.
* On Windows, ``fcntl`` is not available; the implementation falls back to
  a best-effort no-op that logs a warning.  Actual multi-process Windows
  support requires ``msvcrt.locking`` which is out of scope for V1.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType

logger = logging.getLogger(__name__)

try:
    import fcntl

    _HAVE_FCNTL = True
except ImportError:  # pragma: no cover — Windows only
    _HAVE_FCNTL = False


class LockAcquisitionError(OSError):
    """Raised when the lock cannot be acquired (e.g. already held)."""


class RuntimeLock:
    """Advisory POSIX file lock for single-writer enforcement.

    Args:
        lock_path: Path to the lock file.  Created if it does not exist.
        owner_tag: Short string written to the lock file for diagnostics
                   (e.g. a command name or run ID).
    """

    def __init__(self, lock_path: Path, owner_tag: str = "") -> None:
        self.lock_path = lock_path
        self.owner_tag = owner_tag
        self._fd: int | None = None

    # ------------------------------------------------------------------
    # Context-manager interface
    # ------------------------------------------------------------------

    def __enter__(self) -> RuntimeLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.release()

    # ------------------------------------------------------------------
    # Low-level acquire / release
    # ------------------------------------------------------------------

    def acquire(self) -> None:
        """Acquire the exclusive file lock, blocking until it is available.

        Raises:
            LockAcquisitionError: If the lock file cannot be opened/created.
            RuntimeError: If this instance already holds the lock.
        """
        if self._fd is not None:
            raise RuntimeError(
                f"RuntimeLock.acquire() called while already holding lock on {self.lock_path}"
            )

        if not _HAVE_FCNTL:  # pragma: no cover
            logger.warning(
                "runtime_lock_no_fcntl",
                extra={"lock_path": str(self.lock_path), "owner_tag": self.owner_tag},
            )
            return

        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(str(self.lock_path), os.O_CREAT | os.O_WRONLY)
        except OSError as exc:
            raise LockAcquisitionError(f"cannot open lock file {self.lock_path}: {exc}") from exc

        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError as exc:  # pragma: no cover — rare kernel error
            os.close(fd)
            raise LockAcquisitionError(f"flock failed on {self.lock_path}: {exc}") from exc

        # Write owner info for diagnostics
        try:
            tag = f"pid={os.getpid()} cmd={self.owner_tag}\n".encode()
            os.write(fd, tag)
        except OSError:  # pragma: no cover — write failure is non-fatal
            pass

        self._fd = fd
        logger.debug(
            "runtime_lock_acquired",
            extra={
                "lock_path": str(self.lock_path),
                "owner_tag": self.owner_tag,
                "pid": os.getpid(),
            },
        )

    def release(self) -> None:
        """Release the lock and close the file descriptor.

        Safe to call even if the lock was never acquired or already released.
        """
        if not _HAVE_FCNTL:  # pragma: no cover
            return

        if self._fd is None:
            return

        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
        except OSError:  # pragma: no cover — ignore errors on release
            pass
        finally:
            self._fd = None

        logger.debug(
            "runtime_lock_released",
            extra={"lock_path": str(self.lock_path), "owner_tag": self.owner_tag},
        )

    @property
    def is_held(self) -> bool:
        """``True`` if this instance currently holds the lock."""
        return self._fd is not None
