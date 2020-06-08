import os
import uuid
from typing import Any, Callable

BytesFunction = Callable[[bytes], Any]
MAX_ATTEMPTS = 5


class RecoverableFunction:
    def __init__(self, directory: str, f: BytesFunction):
        self.directory = directory
        self.f = f

    def generate_filename(self) -> str:
        return uuid.uuid4().hex

    def __call__(self, content: bytes):
        fd = None
        attempts = 0
        while fd is None and attempts < MAX_ATTEMPTS:
            attempts += 1
            filename = self.generate_filename()
            path = os.path.join(self.directory, filename)
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_EXLOCK)
            except Exception:
                continue

        if fd is not None:
            os.write(fd, content)
            os.fsync(fd)

        try:
            ret = self.f(content)
        except Exception:
            if fd is not None:
                os.close(fd)
            raise
        else:
            if fd is not None:
                os.unlink(path)
                os.close(fd)
            return ret

    def listdir(self):
        return os.listdir(self.directory)

    def recover_from_filename(self, filename: str):
        path = os.path.join(self.directory, filename)
        fd = os.open(path, os.O_RDONLY | os.O_EXLOCK | os.O_NONBLOCK)
        file = os.fdopen(fd, 'rb')
        content = file.read()
        try:
            ret = self.f(content)
        except Exception:
            file.close()
            raise
        else:
            os.unlink(path)
            file.close()
            return ret


def recoverable(directory: str) -> Callable[[BytesFunction], RecoverableFunction]:
    def decorator(f: BytesFunction) -> RecoverableFunction:
        return RecoverableFunction(directory, f)
    return decorator
