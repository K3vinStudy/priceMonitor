import io
import os
import sys
from datetime import datetime
from pathlib import Path

from PySide6.QtWidgets import QApplication

from gui.main_window import MainWindow


class TeeStream:
    def __init__(self, original_stream, log_file):
        self.original_stream = original_stream
        self.log_file = log_file
        self.encoding = getattr(original_stream, "encoding", "utf-8")
        self.errors = getattr(original_stream, "errors", "replace")

    def write(self, message):
        if not isinstance(message, str):
            message = str(message)

        self.original_stream.write(message)
        self.log_file.write(message)
        self.flush()
        return len(message)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        try:
            self.original_stream.flush()
        except Exception:
            pass
        try:
            self.log_file.flush()
        except Exception:
            pass

    def isatty(self):
        try:
            return self.original_stream.isatty()
        except Exception:
            return False

    def fileno(self):
        return self.original_stream.fileno()


class _LogResources:
    def __init__(self, log_file, stdout_dup, stderr_dup):
        self.log_file = log_file
        self.stdout_dup = stdout_dup
        self.stderr_dup = stderr_dup

    def close(self):
        for fd in (self.stdout_dup, self.stderr_dup):
            try:
                os.close(fd)
            except Exception:
                pass
        try:
            self.log_file.close()
        except Exception:
            pass


def setup_cli_logging():
    log_dir = Path.cwd() / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / f"cli-{datetime.now().strftime('%Y-%m-%d')}.log"
    log_file = log_path.open("a", encoding="utf-8", buffering=1)

    stdout_dup = os.dup(1)
    stderr_dup = os.dup(2)

    terminal_stdout = io.TextIOWrapper(os.fdopen(stdout_dup, "wb", closefd=False), encoding="utf-8", errors="replace", line_buffering=True)
    terminal_stderr = io.TextIOWrapper(os.fdopen(stderr_dup, "wb", closefd=False), encoding="utf-8", errors="replace", line_buffering=True)

    # 低层 fd 1/2 也重定向到日志文件，尽量捕获原生扩展和解释器直接写到 stdout/stderr 的内容。
    os.dup2(log_file.fileno(), 1)
    os.dup2(log_file.fileno(), 2)

    sys.stdout = TeeStream(terminal_stdout, log_file)
    sys.stderr = TeeStream(terminal_stderr, log_file)

    return _LogResources(log_file, stdout_dup, stderr_dup)


def main():
    log_resources = setup_cli_logging()
    try:
        app = QApplication(sys.argv)
        window = MainWindow()
        window.show()
        sys.exit(app.exec())
    finally:
        try:
            log_resources.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()