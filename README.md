# GPU Monitor

Lightweight web dashboard that reads SSH hosts and shows `nvidia-smi` GPU stats.

## Run

```powershell
python server.py
```

Then open `http://localhost:8000`.

## Configuration

- `SSH_CONFIG_PATH`: Path to your SSH config. Defaults to `~/.ssh/config`.
- `PORT`: HTTP port (default `8000`).
- `SSH_CONTROL_PATH`: Enable SSH multiplexing (non-Windows OpenSSH only), example `~/.ssh/cm-%r@%h:%p`.
- `SSH_CONTROL_PERSIST`: ControlPersist value (default `60s`).
