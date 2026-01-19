import csv
import json
import mimetypes
import os
import pathlib
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE_DIR = pathlib.Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
SSH_CONFIG_PATH = os.environ.get(
    "SSH_CONFIG_PATH", os.path.expanduser("~/.ssh/config")
)
SSH_CONTROL_PATH = os.environ.get("SSH_CONTROL_PATH")
if SSH_CONTROL_PATH:
    SSH_CONTROL_PATH = os.path.expanduser(SSH_CONTROL_PATH)
SSH_CONTROL_PERSIST = os.environ.get("SSH_CONTROL_PERSIST", "60s")
SSH_USE_CONTROL = bool(SSH_CONTROL_PATH) and os.name != "nt"
GPU_QUERY = (
    "nvidia-smi --query-gpu=index,name,temperature.gpu,"
    "utilization.gpu,memory.used,memory.total "
    "--format=csv,noheader,nounits"
)
GPU_PROCESS_SCRIPT = r"""
import csv
import json
import os
import subprocess
import sys


def run(cmd):
    process = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if process.returncode != 0:
        sys.stderr.write(process.stderr or process.stdout)
        sys.exit(process.returncode)
    return process.stdout.strip()


def parse_gpu_map(text):
    mapping = {}
    for row in csv.reader(text.splitlines()):
        if len(row) < 2:
            continue
        index, uuid = [item.strip() for item in row[:2]]
        mapping[uuid] = index
    return mapping


def parse_processes(text, mapping):
    processes = []
    if not text:
        return processes
    for row in csv.reader(text.splitlines()):
        if len(row) < 4:
            continue
        uuid, pid, name, mem = [item.strip() for item in row[:4]]
        gpu_index = mapping.get(uuid, "")
        cwd = ""
        cwd_error = ""
        if pid.isdigit():
            try:
                cwd = os.readlink("/proc/{}/cwd".format(pid))
            except Exception as exc:
                cwd_error = str(exc)
        mem_used = None
        if mem:
            try:
                mem_used = int(float(mem))
            except ValueError:
                mem_used = None
        processes.append(
            {
                "gpu_index": int(gpu_index) if gpu_index.isdigit() else None,
                "pid": int(pid) if pid.isdigit() else None,
                "name": name,
                "mem_used": mem_used,
                "cwd": cwd,
                "cwd_error": cwd_error,
            }
        )
    return processes


try:
    gpu_text = run(
        "nvidia-smi --query-gpu=index,uuid --format=csv,noheader,nounits"
    )
    proc_text = run(
        "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_memory "
        "--format=csv,noheader,nounits"
    )
except Exception as exc:
    sys.stderr.write(str(exc))
    sys.exit(1)

if proc_text.strip().lower().startswith("no running processes"):
    proc_text = ""

mapping = parse_gpu_map(gpu_text)
processes = parse_processes(proc_text, mapping)
print(json.dumps(processes))
"""


def parse_ssh_config(path_str):
    path = pathlib.Path(path_str)
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return []

    hosts = []
    seen = set()
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if not parts:
            continue
        if parts[0].lower() == "host":
            for host in parts[1:]:
                if any(ch in host for ch in "*?!"):
                    continue
                if host not in seen:
                    seen.add(host)
                    hosts.append(host)
    return hosts


def _run_ssh(host):
    cmd = [
        "ssh",
        "-F",
        SSH_CONFIG_PATH,
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
    ]
    if SSH_USE_CONTROL:
        cmd.extend(
            [
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPersist={SSH_CONTROL_PERSIST}",
                "-o",
                f"ControlPath={SSH_CONTROL_PATH}",
            ]
        )
    cmd.extend([host, GPU_QUERY])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"host": host, "ok": False, "error": "ssh timed out", "gpus": []}

    if result.returncode != 0:
        error_text = (result.stderr or result.stdout or "").strip()
        if not error_text:
            error_text = f"ssh exited with {result.returncode}"
        return {"host": host, "ok": False, "error": error_text, "gpus": []}

    output = result.stdout.strip()
    if not output:
        return {"host": host, "ok": False, "error": "no data from nvidia-smi", "gpus": []}

    gpus = []
    reader = csv.reader(output.splitlines())
    for row in reader:
        row = [item.strip() for item in row]
        if len(row) < 6:
            continue
        try:
            index = int(row[0])
            name = row[1]
            temp = int(float(row[2]))
            util = int(float(row[3]))
            mem_used = int(float(row[4]))
            mem_total = int(float(row[5]))
        except ValueError:
            continue
        gpus.append(
            {
                "index": index,
                "name": name,
                "temp": temp,
                "util": util,
                "mem_used": mem_used,
                "mem_total": mem_total,
            }
        )

    if not gpus:
        return {
            "host": host,
            "ok": False,
            "error": "unable to parse nvidia-smi output",
            "gpus": [],
        }

    util_avg = round(sum(gpu["util"] for gpu in gpus) / len(gpus))
    mem_used_total = sum(gpu["mem_used"] for gpu in gpus)
    mem_total_total = sum(gpu["mem_total"] for gpu in gpus)
    mem_pct = round((mem_used_total / mem_total_total) * 100) if mem_total_total else 0

    return {
        "host": host,
        "ok": True,
        "summary": {
            "count": len(gpus),
            "util_avg": util_avg,
            "mem_used": mem_used_total,
            "mem_total": mem_total_total,
            "mem_pct": mem_pct,
        },
        "gpus": gpus,
    }


def _run_ssh_processes(host):
    cmd = [
        "ssh",
        "-F",
        SSH_CONFIG_PATH,
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
    ]
    if SSH_USE_CONTROL:
        cmd.extend(
            [
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPersist={SSH_CONTROL_PERSIST}",
                "-o",
                f"ControlPath={SSH_CONTROL_PATH}",
            ]
        )
    cmd.extend(
        [
            host,
            "sh",
            "-c",
            "command -v python3 >/dev/null 2>&1 && exec python3 - || exec python -",
        ]
    )
    try:
        result = subprocess.run(
            cmd,
            input=GPU_PROCESS_SCRIPT,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"host": host, "ok": False, "error": "ssh timed out", "processes": []}

    if result.returncode != 0:
        error_text = (result.stderr or result.stdout or "").strip()
        if not error_text:
            error_text = f"ssh exited with {result.returncode}"
        return {"host": host, "ok": False, "error": error_text, "processes": []}

    output = result.stdout.strip()
    if not output:
        return {"host": host, "ok": True, "processes": []}

    try:
        processes = json.loads(output)
    except json.JSONDecodeError:
        return {
            "host": host,
            "ok": False,
            "error": "invalid process data",
            "processes": [],
        }

    return {"host": host, "ok": True, "processes": processes}


def fetch_gpu_processes(host, index):
    result = _run_ssh_processes(host)
    if not result.get("ok"):
        result["index"] = index
        return result
    processes = result.get("processes", [])
    filtered = [item for item in processes if item.get("gpu_index") == index]
    return {"host": host, "ok": True, "index": index, "processes": filtered}


def fetch_statuses(hosts):
    if not hosts:
        return []

    max_workers = min(8, len(hosts))
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_run_ssh, host): host for host in hosts}
        for future in as_completed(future_map):
            host = future_map[future]
            try:
                results[host] = future.result()
            except Exception as exc:
                results[host] = {
                    "host": host,
                    "ok": False,
                    "error": f"error: {exc}",
                    "gpus": [],
                }

    ordered = []
    for host in hosts:
        ordered.append(results.get(host, {"host": host, "ok": False, "error": "missing"}))
    return ordered


class GPURequestHandler(BaseHTTPRequestHandler):
    def _safe_write(self, data):
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return False
        return True

    def _send_json(self, payload, status=HTTPStatus.OK):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self._safe_write(data)

    def _send_text(self, message, status=HTTPStatus.BAD_REQUEST):
        data = message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self._safe_write(data)

    def _serve_static(self, rel_path):
        if rel_path == "/":
            rel_path = "/index.html"
        candidate = (WEB_DIR / rel_path.lstrip("/")).resolve()
        try:
            candidate.relative_to(WEB_DIR)
        except ValueError:
            self._send_text("invalid path", status=HTTPStatus.NOT_FOUND)
            return

        if not candidate.is_file():
            self._send_text("not found", status=HTTPStatus.NOT_FOUND)
            return

        mime_type, _ = mimetypes.guess_type(str(candidate))
        if not mime_type:
            mime_type = "application/octet-stream"
        data = candidate.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self._safe_write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/servers":
            hosts = parse_ssh_config(SSH_CONFIG_PATH)
            self._send_json({"hosts": hosts, "config": SSH_CONFIG_PATH})
            return
        if parsed.path == "/api/status":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            if not host:
                self._send_text("missing host", status=HTTPStatus.BAD_REQUEST)
                return
            status = fetch_statuses([host])[0]
            self._send_json(status)
            return
        if parsed.path == "/api/gpu-processes":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            index_raw = (query.get("index") or [None])[0]
            if not host or index_raw is None:
                self._send_json(
                    {"ok": False, "error": "missing host or index"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            try:
                index = int(index_raw)
            except ValueError:
                self._send_json(
                    {"ok": False, "error": "invalid index"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            result = fetch_gpu_processes(host, index)
            self._send_json(result)
            return
        self._serve_static(parsed.path)

    def do_POST(self):
        if self.path != "/api/status":
            self._send_text("not found", status=HTTPStatus.NOT_FOUND)
            return

        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            self._send_text("invalid json", status=HTTPStatus.BAD_REQUEST)
            return

        hosts = payload.get("hosts")
        if not isinstance(hosts, list) or not all(isinstance(h, str) for h in hosts):
            hosts = parse_ssh_config(SSH_CONFIG_PATH)

        results = fetch_statuses(hosts)
        self._send_json({"results": results})


def main():
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), GPURequestHandler)
    print(f"GPU Monitor running on http://localhost:{port}")
    print(f"Using SSH config: {SSH_CONFIG_PATH}")
    server.serve_forever()


if __name__ == "__main__":
    main()
