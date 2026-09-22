# Klipper / Moonraker Bridge for IRIS Home

Bridges any Klipper-based 3D printer running [Moonraker](https://moonraker.readthedocs.io/)
to IRIS Home. Supports multiple printers from one bridge process — each gets
its own entry in `config.json` and its own IRIS device / WebSocket connection.

## How it's structured:

- `KlipperController` — one instance per printer, wraps that printer's
  Moonraker HTTP API. Every method returns a `Result(success, message, data)`.
- `functions` dict — maps command names (`get_status`, `home`, `start_print`,
  etc.) to controller methods, used to dispatch commands coming from the
  IRIS backend over the WebSocket.
- `ws_loop` — one per printer. Connects to
  `wss://backend.irisapis.us/api/devices/ws/{user}/{device_id}`, polls
  Moonraker every `STATE_POLL_INTERVAL` seconds and pushes state changes
  (or a heartbeat), and runs commands sent from the backend.

Since Klipper printers live at a known network address rather than being
broadcast-discoverable like WiZ bulbs, there's no `discover()` step — instead
`main()` reads the printer list from `config.json` and starts one `ws_loop`
task per printer.

## Setup

1. Fill in one entry per printer under `printers`, each with a unique `device_id`,
     a `name`, and the `moonraker_url` (e.g. `http://<pi-ip>:7125`).
     `api_key` is only needed if you've enabled Moonraker's API key auth.
2. `pip install -r requirements.txt`
3. Run: `python3 main.py`

For your Ender 3 V3 SE on the Rpi4, if the bridge runs on the *same* Pi as
Moonraker, `http://localhost:7125` works. If it runs elsewhere on your
network, use the Pi's LAN IP or `.local` hostname instead.

## Commands exposed

| cmd | params | what it does |
|---|---|---|
| `get_status` | — | print state, progress %, temps, filename, etc. |
| `get_info` | — | Klippy host state, hostname, firmware version |
| `send_gcode` | `script` | run raw G-code |
| `home` | `axes` (optional, e.g. `"XY"`) | `G28`, all axes if omitted |
| `move` | `x, y, z, e, feedrate, relative` | jog the toolhead |
| `set_extruder_temp` | `temp, tool` | `M104` |
| `set_bed_temp` | `temp` | `M140` |
| `set_fan_speed` | `percent` (0–100) | `M106` |
| `turn_off_heaters` | — | kills all heaters |
| `list_files` | — | gcode files on the printer |
| `start_print` | `filename` | starts a print |
| `pause_print` / `resume_print` / `cancel_print` | — | print job control |
| `emergency_stop` | — | immediate halt (`M112`-equivalent) |
| `firmware_restart` | — | restarts Klipper + resets MCUs |

## One thing to double-check on your end

The WiZ bridge's device struct uses `"type": "leds"` with `status: "on"/"off"`
and `value` = brightness — that's a schema specific to light devices. For a
printer there's no equivalent example to copy, so `build_device()` in
`main.py` guesses at a reasonable shape:

```json
{
  "id": "ender3-v3-se",
  "name": "Ender 3 V3 SE",
  "type": "printer",
  "status": "printing",
  "value": "42.3",
  "metadata": { "hotend_temp": 210.1, "bed_temp": 60.0, "filename": "benchy.gcode", ... }
}
```

`status` mirrors Klipper's own `print_stats.state` values
(`standby` / `printing` / `paused` / `complete` / `error` / `cancelled`),
and `value` is print progress as a percentage. If your IRIS backend/frontend
expects a different `type` string or status vocabulary for non-light
devices, that's the one spot in the file to adjust
(`build_device()`, near the bottom of `main.py`).

## Notes

- Moonraker also has a WebSocket JSON-RPC API with push notifications
  (`printer.objects.subscribe`), which would be lower-latency than polling.
  This bridge polls instead, to keep the exact same connection-and-retry
  shape as your WiZ bridge — but it's a reasonable upgrade later if 2-second
  polling feels laggy for something like live temperature graphs.
- Adding another Klipper printer (Prusa/Voron/whatever, as long as it runs
  Moonraker) is just another entry in `config.json`'s `printers` list — no
  code changes.
