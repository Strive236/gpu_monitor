import csv
import json
import mimetypes
import os
import pathlib
import subprocess
import shutil
import sys
import tempfile
import uuid
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
SSH_CONNECT_TIMEOUT = int(os.environ.get("SSH_CONNECT_TIMEOUT", "15"))
SSH_FILE_TIMEOUT = int(os.environ.get("SSH_FILE_TIMEOUT", "45"))
SSH_COMMAND_TIMEOUT = int(os.environ.get("SSH_COMMAND_TIMEOUT", "45"))
SSH_COMMAND_OUTPUT_LIMIT = int(os.environ.get("SSH_COMMAND_OUTPUT_LIMIT", "20000"))
SSH_COMMAND_COMPLETION_LIMIT = int(os.environ.get("SSH_COMMAND_COMPLETION_LIMIT", "200"))
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
SCHEDULED_TASK_NAME = "GPU Monitor"


def _run_schtasks(args):
    system_root = os.environ.get("SystemRoot", r"C:\Windows")
    schtasks_path = os.path.join(system_root, "System32", "schtasks.exe")
    if os.path.isfile(schtasks_path):
        cmd = [schtasks_path]
    else:
        cmd = ["schtasks"]
    return subprocess.run(
        cmd + args,
        capture_output=True,
        text=True,
        check=False,
    )


def _powershell_exe():
    return shutil.which("powershell") or shutil.which("pwsh")


def _ps_quote(value):
    return "'" + value.replace("'", "''") + "'"


def _run_powershell(script):
    exe = _powershell_exe()
    if not exe:
        return None, "powershell not found"
    result = subprocess.run(
        [exe, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        error_text = (result.stderr or result.stdout or "").strip()
        return result, error_text or f"powershell exited {result.returncode}"
    return result, ""


def _query_task_powershell(task_name):
    name = _ps_quote(task_name)
    script = "\n".join(
        [
            f"$task = Get-ScheduledTask -TaskName {name} -ErrorAction SilentlyContinue",
            "if ($null -eq $task) { exit 0 }",
            f"$info = Get-ScheduledTaskInfo -TaskName {name} -ErrorAction SilentlyContinue",
            "$state = $null",
            "if ($info -and $info.State) { $state = $info.State.ToString() }",
            "elseif ($task.State) { $state = $task.State.ToString() }",
            "@{ enabled = $true; state = $state } | ConvertTo-Json -Compress",
        ]
    )
    result, error_text = _run_powershell(script)
    if error_text:
        return None, error_text
    output = (result.stdout or "").strip() if result else ""
    if not output:
        return {"enabled": False}, ""
    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return None, "invalid task status output"
    return data, ""


def _query_schtasks(task_name):
    result = _run_schtasks(["/Query", "/TN", task_name, "/FO", "LIST", "/V"])
    if result.returncode != 0:
        return None, (result.stderr or result.stdout or "").strip()
    data = {}
    for raw in (result.stdout or "").splitlines():
        if ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        data[key.strip()] = value.strip()
    return data, ""


def _startup_status():
    if os.name != "nt":
        return {"ok": False, "supported": False, "enabled": False}
    if _powershell_exe():
        data, error_text = _query_task_powershell(SCHEDULED_TASK_NAME)
        if error_text:
            return {
                "ok": False,
                "supported": True,
                "enabled": False,
                "error": error_text,
            }
        if data and data.get("enabled"):
            return {
                "ok": True,
                "supported": True,
                "enabled": True,
                "status": data.get("state", ""),
                "task": SCHEDULED_TASK_NAME,
            }
        return {"ok": True, "supported": True, "enabled": False}
    info, error_text = _query_schtasks(SCHEDULED_TASK_NAME)
    if info is None:
        if "cannot find" in error_text.lower():
            return {"ok": True, "supported": True, "enabled": False}
        return {"ok": False, "supported": True, "enabled": False, "error": error_text}
    status = info.get("Status", "") or info.get("State", "")
    return {
        "ok": True,
        "supported": True,
        "enabled": True,
        "status": status,
        "task": info.get("TaskName", SCHEDULED_TASK_NAME),
    }


def _set_startup(enabled):
    if os.name != "nt":
        return {"ok": False, "error": "startup not supported"}
    if enabled:
        script_path = pathlib.Path(__file__).resolve()
        python_exe = sys.executable
        if not os.path.isfile(python_exe):
            return {"ok": False, "error": f"python not found: {python_exe}"}
        if not script_path.is_file():
            return {"ok": False, "error": f"script not found: {script_path}"}
        if _powershell_exe():
            exe = _ps_quote(python_exe)
            script = _ps_quote(str(script_path))
            workdir = _ps_quote(str(script_path.parent))
            name = _ps_quote(SCHEDULED_TASK_NAME)
            ps_script = "\n".join(
                [
                    f"$exe = {exe}",
                    f"$script = {script}",
                    f"$work = {workdir}",
                    "$user = if ($env:USERDOMAIN) { $env:USERDOMAIN + '\\\\' + $env:USERNAME } else { $env:USERNAME }",
                    "$arg = '\"' + $script + '\"'",
                    "$action = New-ScheduledTaskAction -Execute $exe -Argument $arg -WorkingDirectory $work",
                    "$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user",
                    f"Register-ScheduledTask -TaskName {name} -Action $action -Trigger $trigger -User $user -RunLevel Limited -Description 'GPU Monitor' -Force | Out-Null",
                ]
            )
            result, error_text = _run_powershell(ps_script)
            if error_text:
                return {"ok": False, "error": error_text}
            return {"ok": True, "enabled": True}
        task_xml = f"""<?xml version="1.0" encoding="utf-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>GPU Monitor</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{python_exe}</Command>
      <Arguments>"{script_path}"</Arguments>
      <WorkingDirectory>{script_path.parent}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"""
        task_file = BASE_DIR / ".gpu_monitor_task.xml"
        try:
            task_file.write_text(task_xml, encoding="utf-16")
        except OSError as exc:
            return {"ok": False, "error": str(exc)}
        result = _run_schtasks(
            ["/Create", "/TN", SCHEDULED_TASK_NAME, "/XML", str(task_file), "/F"]
        )
        try:
            task_file.unlink()
        except OSError:
            pass
        if result.returncode != 0:
            error_text = (result.stderr or result.stdout or "").strip()
            return {
                "ok": False,
                "error": error_text or "failed to create task",
                "details": {
                    "python": python_exe,
                    "script": str(script_path),
                    "task_xml": task_xml,
                },
            }
        return {"ok": True, "enabled": True}
    if _powershell_exe():
        name = _ps_quote(SCHEDULED_TASK_NAME)
        ps_script = f"Unregister-ScheduledTask -TaskName {name} -Confirm:$false -ErrorAction SilentlyContinue"
        result, error_text = _run_powershell(ps_script)
        if error_text:
            return {"ok": False, "error": error_text}
        return {"ok": True, "enabled": False}
    result = _run_schtasks(["/Delete", "/TN", SCHEDULED_TASK_NAME, "/F"])
    if result.returncode != 0:
        error_text = (result.stderr or result.stdout or "").strip()
        return {"ok": False, "error": error_text or "failed to delete task"}
    return {"ok": True, "enabled": False}


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


def parse_ssh_config_users(path_str):
    path = pathlib.Path(path_str)
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return {}, ""

    user_map = {}
    default_user = ""
    current_hosts = []
    current_has_wildcard = False
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if not parts:
            continue
        keyword = parts[0].lower()
        if keyword == "host":
            current_hosts = []
            current_has_wildcard = False
            for host in parts[1:]:
                if any(ch in host for ch in "*?!"):
                    current_has_wildcard = True
                    continue
                current_hosts.append(host)
            continue
        if keyword == "user" and len(parts) >= 2:
            user_value = parts[1]
            if current_hosts:
                for host in current_hosts:
                    user_map.setdefault(host, user_value)
            elif current_has_wildcard and not default_user:
                default_user = user_value

    return user_map, default_user


SSH_USER_MAP, SSH_DEFAULT_USER = parse_ssh_config_users(SSH_CONFIG_PATH)


def _ssh_user_for_host(host):
    if not host:
        return ""
    return SSH_USER_MAP.get(host) or SSH_DEFAULT_USER


def _ssh_base_cmd(host=None):
    cmd = [
        "ssh",
        "-F",
        SSH_CONFIG_PATH,
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
        "-o",
        "ClearAllForwardings=yes",
    ]
    user = _ssh_user_for_host(host)
    if user:
        cmd.extend(["-o", f"User={user}"])
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
    return cmd


def _sftp_base_cmd(host=None):
    cmd = [
        "sftp",
        "-F",
        SSH_CONFIG_PATH,
        "-o",
        f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
        "-o",
        "BatchMode=yes",
        "-o",
        "ClearAllForwardings=yes",
    ]
    user = _ssh_user_for_host(host)
    if user:
        cmd.extend(["-o", f"User={user}"])
    return cmd


def _quote_sh(value):
    if value is None:
        return "''"
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _quote_sftp_path(value):
    if value is None:
        return '""'
    return '"' + value.replace('"', '\\"') + '"'


def _sftp_local_path(path):
    return path.replace("\\", "/")


def _ssh_error_text(result):
    return (result.stderr or result.stdout or "").strip()


def _trim_output(text, limit):
    if not text:
        return ""
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit] + "\n... (truncated)"


def _run_ssh_command(host, command, cwd=None):
    marker = f"__GPU_MONITOR_PWD__{uuid.uuid4().hex}__"
    prefix = f"cd {_quote_sh(cwd)} && " if cwd else ""
    trailer = f'code=$?; printf "\\n{marker}%s|%s\\n" "$code" "$PWD"'
    full_command = f"{prefix}{command}\n{trailer}"
    cmd = _ssh_base_cmd(host)
    cmd.extend([host, "bash", "-lc", _quote_sh(full_command)])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SSH_COMMAND_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "error": "ssh timed out",
            "exit_code": None,
            "stdout": "",
            "stderr": "",
            "cwd": cwd or "",
        }

    stdout_raw = result.stdout or ""
    stderr_raw = result.stderr or ""
    cwd_value = None
    exit_code = result.returncode
    if marker in stdout_raw:
        before, _, after = stdout_raw.rpartition(marker)
        stdout_raw = before
        after_lines = after.splitlines()
        if after_lines:
            marker_line = after_lines[0].strip()
            if marker_line:
                parts = marker_line.split("|", 1)
                if parts and parts[0].isdigit():
                    exit_code = int(parts[0])
                if len(parts) > 1:
                    cwd_value = parts[1].strip()

    stdout = _trim_output(stdout_raw.strip(), SSH_COMMAND_OUTPUT_LIMIT)
    stderr = _trim_output(stderr_raw.strip(), SSH_COMMAND_OUTPUT_LIMIT)
    if exit_code != 0:
        error_text = _ssh_error_text(result) or f"command exited with {exit_code}"
        return {
            "ok": False,
            "error": error_text,
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
            "cwd": cwd_value or cwd or "",
        }
    return {
        "ok": True,
        "exit_code": exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "cwd": cwd_value or cwd or "",
    }


def _run_ssh_completion(host, prefix, cwd=None, mode="file"):
    cmd = _ssh_base_cmd(host)
    quoted_prefix = _quote_sh(prefix or "")
    cd_prefix = f"cd {_quote_sh(cwd)} && " if cwd else ""
    if mode == "command":
        complete_cmd = f"{cd_prefix}compgen -c -- {quoted_prefix}"
    else:
        complete_cmd = f"{cd_prefix}compgen -f -- {quoted_prefix}"
    cmd.extend([host, "bash", "-lc", _quote_sh(complete_cmd)])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return [], "ssh timed out"
    if result.returncode != 0:
        error_text = _ssh_error_text(result)
        if not error_text:
            error_text = f"ssh exited with {result.returncode}"
        return [], error_text
    matches = []
    seen = set()
    for line in result.stdout.splitlines():
        item = line.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        matches.append(item)
        if len(matches) >= SSH_COMMAND_COMPLETION_LIMIT:
            break
    return matches, ""


def _run_ssh(host):
    cmd = _ssh_base_cmd(host)
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
        error_text = _ssh_error_text(result)
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
    cmd = _ssh_base_cmd(host)
    remote_cmd = (
        "command -v python3 >/dev/null 2>&1 && exec python3 - || exec python -"
    )
    cmd.extend(
        [
            host,
            "sh",
            "-c",
            _quote_sh(remote_cmd),
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
        error_text = _ssh_error_text(result)
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


def _remote_file_size(host, remote_path):
    cmd = _ssh_base_cmd(host)
    quoted = _quote_sh(remote_path)
    cmd.extend([host, "sh", "-c", _quote_sh(f"ls -ln -- {quoted}")])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SSH_FILE_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, "ssh timed out"

    if result.returncode != 0:
        error_text = _ssh_error_text(result) or f"ssh exited with {result.returncode}"
        return None, error_text

    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        return None, "invalid file size"
    parts = lines[-1].split()
    if len(parts) < 5:
        return None, "invalid file size"
    size_text = parts[4]
    try:
        return int(size_text), ""
    except ValueError:
        return None, "invalid file size"


def _upload_via_ssh(host, remote_path, source, length):
    cmd = _ssh_base_cmd(host)
    quoted = _quote_sh(remote_path)
    cmd.extend([host, "sh", "-c", _quote_sh(f"cat > {quoted}")])
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    remaining = length
    error_text = ""
    try:
        while remaining > 0:
            chunk = source.read(min(65536, remaining))
            if not chunk:
                break
            proc.stdin.write(chunk)
            remaining -= len(chunk)
    except BrokenPipeError:
        error_text = "ssh failed during upload"
    except ConnectionResetError:
        error_text = "client disconnected"
    finally:
        if proc.stdin:
            try:
                proc.stdin.close()
            except Exception:
                pass

    try:
        stdout, stderr = proc.communicate(timeout=300)
    except subprocess.TimeoutExpired:
        proc.kill()
        return {"ok": False, "error": "upload timed out"}

    if remaining > 0 and not error_text:
        error_text = "upload interrupted"
    if proc.returncode != 0 and not error_text:
        error_text = (stderr or stdout or b"").decode("utf-8", errors="ignore").strip()
        if not error_text:
            error_text = f"ssh exited with {proc.returncode}"

    if error_text:
        return {"ok": False, "error": error_text}
    return {"ok": True}


def _download_via_sftp(host, remote_path):
    temp_dir = tempfile.mkdtemp(prefix="gpu_monitor_")
    tmp_path = os.path.join(temp_dir, f"download_{uuid.uuid4().hex}")
    local_path = _sftp_local_path(tmp_path)
    cmd = _sftp_base_cmd(host)
    cmd.extend(["-b", "-", host])
    script = f"get {_quote_sftp_path(remote_path)} {_quote_sftp_path(local_path)}\n"
    try:
        result = subprocess.run(
            cmd,
            input=script.encode("utf-8"),
            capture_output=True,
            timeout=SSH_FILE_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        try:
            os.rmdir(temp_dir)
        except OSError:
            pass
        return None, None, "ssh timed out"
    if result.returncode != 0:
        error_text = _ssh_error_text(result)
        if not error_text:
            error_text = f"sftp exited with {result.returncode}"
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        try:
            os.rmdir(temp_dir)
        except OSError:
            pass
        return None, None, error_text
    return tmp_path, temp_dir, ""

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
        if parsed.path == "/api/startup":
            self._send_json(_startup_status())
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
        if parsed.path == "/api/download":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            remote_path = (query.get("path") or [None])[0]
            if not host or not remote_path:
                self._send_json(
                    {"ok": False, "error": "missing host or path"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            filename = os.path.basename(remote_path) or "download.bin"
            safe_name = filename.replace('"', "_")
            tmp_path, temp_dir, error_text = _download_via_sftp(host, remote_path)
            if error_text:
                self._send_json(
                    {"ok": False, "error": error_text},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            try:
                file_size = os.path.getsize(tmp_path)
            except OSError:
                file_size = None
            try:
                stream = open(tmp_path, "rb")
            except OSError as exc:
                self._send_json(
                    {"ok": False, "error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                if temp_dir:
                    try:
                        os.rmdir(temp_dir)
                    except OSError:
                        pass
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/octet-stream")
            if file_size is not None:
                self.send_header("Content-Length", str(file_size))
            self.send_header("Content-Disposition", f'attachment; filename="{safe_name}"')
            self.end_headers()
            try:
                while True:
                    chunk = stream.read(65536)
                    if not chunk:
                        break
                    if not self._safe_write(chunk):
                        break
            finally:
                try:
                    stream.close()
                except Exception:
                    pass
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                if temp_dir:
                    try:
                        os.rmdir(temp_dir)
                    except OSError:
                        pass
            return
        self._serve_static(parsed.path)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/startup":
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                self._send_text("invalid json", status=HTTPStatus.BAD_REQUEST)
                return
            enabled = payload.get("enabled")
            if not isinstance(enabled, bool):
                self._send_json(
                    {"ok": False, "error": "missing enabled flag"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            result = _set_startup(enabled)
            status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
            self._send_json(result, status=status)
            return
        if parsed.path == "/api/status":
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
            return

        if parsed.path == "/api/command":
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                self._send_text("invalid json", status=HTTPStatus.BAD_REQUEST)
                return
            host = payload.get("host")
            command = payload.get("command")
            if not host or not isinstance(host, str):
                self._send_json(
                    {"ok": False, "error": "missing host"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            if not command or not isinstance(command, str):
                self._send_json(
                    {"ok": False, "error": "missing command"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            cwd = payload.get("cwd")
            if cwd is not None and not isinstance(cwd, str):
                cwd = ""
            result = _run_ssh_command(host, command, cwd=cwd or "")
            self._send_json(result)
            return

        if parsed.path == "/api/command-complete":
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                self._send_text("invalid json", status=HTTPStatus.BAD_REQUEST)
                return
            host = payload.get("host")
            prefix = payload.get("prefix")
            mode = payload.get("mode")
            cwd = payload.get("cwd")
            if not host or not isinstance(host, str):
                self._send_json(
                    {"ok": False, "error": "missing host"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            if prefix is None or not isinstance(prefix, str):
                self._send_json(
                    {"ok": False, "error": "missing prefix"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            if mode not in ("command", "file"):
                mode = "file"
            if cwd is not None and not isinstance(cwd, str):
                cwd = ""
            matches, error_text = _run_ssh_completion(
                host, prefix, cwd=cwd or "", mode=mode
            )
            if error_text:
                self._send_json(
                    {"ok": False, "error": error_text},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            self._send_json({"ok": True, "matches": matches})
            return

        if parsed.path == "/api/upload":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            remote_path = (query.get("path") or [None])[0]
            filename = (query.get("name") or [None])[0]
            if not host or not remote_path:
                self._send_json(
                    {"ok": False, "error": "missing host or path"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            if remote_path.endswith("/"):
                if not filename:
                    self._send_json(
                        {"ok": False, "error": "missing filename"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                remote_path = remote_path + filename

            length_header = self.headers.get("Content-Length")
            if length_header is None:
                self._send_json(
                    {"ok": False, "error": "missing content length"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            try:
                length = int(length_header or 0)
            except ValueError:
                self._send_json(
                    {"ok": False, "error": "invalid content length"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            result = _upload_via_ssh(host, remote_path, self.rfile, length)
            status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
            self._send_json(result, status=status)
            return

        self._send_text("not found", status=HTTPStatus.NOT_FOUND)


def main():
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), GPURequestHandler)
    print(f"GPU Monitor running on http://localhost:{port}")
    print(f"Using SSH config: {SSH_CONFIG_PATH}")
    server.serve_forever()


if __name__ == "__main__":
    main()
