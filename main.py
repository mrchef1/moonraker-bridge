#!/usr/bin/env python3
"""
Klipper / Moonraker Bridge — WebSocket interface for IRIS Home

What IRIS Home actually requires from a bridge:
    1. Open one WebSocket per device to
       wss://backend.irisapis.us/api/devices/ws/{user}/{device_id}
    2. Send a device-update struct over that socket whenever the device's
       state changes (or on a heartbeat), so the backend stays in sync.
    3. Listen on that same socket for commands and act on them.

Everything below is just plain async functions that talk to a printer's
Moonraker HTTP API — no Controller/Result wrapper, since that's not part of
what IRIS needs. Each function either returns a plain JSON-able dict or
raises, and the WS loop's dispatcher turns that into a result/error message
back over the socket.

Since Klipper printers aren't broadcast-discoverable like WiZ bulbs (each is
a Moonraker instance at a known host:port), config.json holds a *list* of
printers, and main() opens one WS connection (one IRIS device) per printer.
"""

import asyncio
import json
import time
import aiohttp
import websockets

from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from functools import partial
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────────────

CONFIG_PATH = Path("/home/iris/hub/config.json")
IRIS_URL = "wss://backend.irisapis.us/api/devices/ws/{user}/{device_id}"

# How often to poll Moonraker for changes made outside Iris
# (Fluidd/Mainsail, a print started from the printer's screen, etc.)
STATE_POLL_INTERVAL = 2.0  # seconds

# Re-send the device state at least this often even if nothing changed,
# so the backend can tell the hub is still alive and stays in sync.
HEARTBEAT_INTERVAL = 30.0  # seconds

# HTTP timeout for calls to Moonraker. Keep this well under
# STATE_POLL_INTERVAL so a slow/unreachable printer can't stall the loop.
HTTP_TIMEOUT = 5.0  # seconds

# Printer objects to poll for status. See:
# https://moonraker.readthedocs.io/en/latest/printer_objects/
STATUS_OBJECTS = [
    "print_stats",
    "toolhead",
    "extruder",
    "heater_bed",
    "virtual_sdcard",
    "fan",
    "display_status",
]


@dataclass
class PrinterConfig:
    device_id: str          # unique id used as the IRIS device id
    name: str                # display name, e.g. "Ender 3 V3 SE"
    moonraker_url: str      # e.g. "http://localhost:7125"
    api_key: Optional[str] = None  # only needed if Moonraker auth is enabled


def load_config():
    data = json.loads(CONFIG_PATH.read_text())
    user = data["user"]
    printers = [
        PrinterConfig(
            device_id=p["device_id"],
            name=p.get("name", p["device_id"]),
            moonraker_url=p["moonraker_url"],
            api_key=p.get("api_key"),
        )
        for p in data["printers"]
    ]
    return user, printers


# ── Moonraker HTTP helpers ───────────────────────────────────────────────────
# Plain functions, not methods — each takes the session/base_url/headers for
# whichever printer is calling it. ws_loop binds these per-printer with
# functools.partial before handing them to the command dispatcher.

async def _get(session: aiohttp.ClientSession, base_url: str, headers: dict,
                path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with session.get(f"{base_url}{path}", params=params,
                            headers=headers, timeout=timeout) as resp:
        resp.raise_for_status()
        body = await resp.json()
        return body.get("result", body)


async def _post(session: aiohttp.ClientSession, base_url: str, headers: dict,
                 path: str, json_body: Optional[Dict[str, Any]] = None,
                 params: Optional[Dict[str, Any]] = None) -> Any:
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with session.post(f"{base_url}{path}", json=json_body, params=params,
                             headers=headers, timeout=timeout) as resp:
        resp.raise_for_status()
        body = await resp.json()
        return body.get("result", body)


# ── Status ───────────────────────────────────────────────────────────────────

async def get_status(session, base_url, headers) -> Dict[str, Any]:
    query = "&".join(STATUS_OBJECTS)
    result = await _get(session, base_url, headers, f"/printer/objects/query?{query}")
    status = result.get("status", {})

    print_stats = status.get("print_stats", {}) or {}
    toolhead = status.get("toolhead", {}) or {}
    extruder = status.get("extruder", {}) or {}
    heater_bed = status.get("heater_bed", {}) or {}
    virtual_sdcard = status.get("virtual_sdcard", {}) or {}
    fan = status.get("fan", {}) or {}
    display_status = status.get("display_status", {}) or {}

    progress = virtual_sdcard.get("progress")
    if progress is None:
        progress = display_status.get("progress")

    return {
        "state": print_stats.get("state", "unknown"),
        "state_message": print_stats.get("message", ""),
        "filename": print_stats.get("filename", ""),
        "progress_pct": round((progress or 0) * 100, 1),
        "print_duration_s": print_stats.get("print_duration"),
        "total_duration_s": print_stats.get("total_duration"),
        "filament_used_mm": print_stats.get("filament_used"),
        "hotend_temp": extruder.get("temperature"),
        "hotend_target": extruder.get("target"),
        "bed_temp": heater_bed.get("temperature"),
        "bed_target": heater_bed.get("target"),
        "fan_speed_pct": round((fan.get("speed") or 0) * 100, 1),
        "position": toolhead.get("position"),
        "homed_axes": toolhead.get("homed_axes"),
    }


async def get_info(session, base_url, headers) -> Dict[str, Any]:
    """Klippy host info: hostname, software version, ready/error/etc."""
    info = await _get(session, base_url, headers, "/printer/info")
    return {
        "state": info.get("state"),
        "state_message": info.get("state_message"),
        "hostname": info.get("hostname"),
        "software_version": info.get("software_version"),
    }


# ── GCode / Motion ───────────────────────────────────────────────────────────

async def send_gcode(session, base_url, headers, script: str) -> Dict[str, Any]:
    if not script or not isinstance(script, str):
        raise ValueError("script must be a non-empty string")
    await _post(session, base_url, headers, "/printer/gcode/script",
                json_body={"script": script})
    return {"ok": True, "message": f"Ran: {script}"}


async def home(session, base_url, headers, axes: Optional[str] = None) -> Dict[str, Any]:
    """axes: e.g. 'XYZ', 'XY', 'Z'. Omit/empty to home all axes."""
    letters = "".join(ch for ch in (axes or "").upper() if ch in "XYZ")
    script = f"G28 {' '.join(letters)}".strip() if letters else "G28"
    return await send_gcode(session, base_url, headers, script)


async def move(
    session, base_url, headers,
    x: Optional[float] = None,
    y: Optional[float] = None,
    z: Optional[float] = None,
    e: Optional[float] = None,
    feedrate: float = 1500,
    relative: bool = True,
) -> Dict[str, Any]:
    """Jog the toolhead. Relative moves (the default) are safest for manual
    jogging since they don't depend on knowing the current position."""
    parts = []
    if x is not None:
        parts.append(f"X{x}")
    if y is not None:
        parts.append(f"Y{y}")
    if z is not None:
        parts.append(f"Z{z}")
    if e is not None:
        parts.append(f"E{e}")
    if not parts:
        raise ValueError("Provide at least one of x/y/z/e")

    mode = "G91" if relative else "G90"
    script = f"{mode}\nG1 {' '.join(parts)} F{feedrate}"
    if relative:
        script += "\nG90"  # restore absolute mode afterward
    return await send_gcode(session, base_url, headers, script)


# ── Temperature / Fan ────────────────────────────────────────────────────────

async def set_extruder_temp(session, base_url, headers, temp: float, tool: int = 0) -> Dict[str, Any]:
    return await send_gcode(session, base_url, headers, f"M104 T{tool} S{temp}")


async def set_bed_temp(session, base_url, headers, temp: float) -> Dict[str, Any]:
    return await send_gcode(session, base_url, headers, f"M140 S{temp}")


async def set_fan_speed(session, base_url, headers, percent: float) -> Dict[str, Any]:
    if not 0 <= percent <= 100:
        raise ValueError("percent must be 0-100")
    return await send_gcode(session, base_url, headers, f"M106 S{round(percent / 100 * 255)}")


async def turn_off_heaters(session, base_url, headers) -> Dict[str, Any]:
    return await send_gcode(session, base_url, headers, "TURN_OFF_HEATERS")


# ── Files ────────────────────────────────────────────────────────────────────

async def list_files(session, base_url, headers) -> Dict[str, Any]:
    files = await _get(session, base_url, headers, "/server/files/list")
    return {"files": [
        {"path": f.get("path"), "size": f.get("size"), "modified": f.get("modified")}
        for f in files
    ]}


# ── Print Job Management ─────────────────────────────────────────────────────

async def start_print(session, base_url, headers, filename: str) -> Dict[str, Any]:
    if not filename:
        raise ValueError("filename is required")
    await _post(session, base_url, headers, "/printer/print/start",
                params={"filename": filename})
    return {"ok": True, "message": f"Started print: {filename}"}


async def pause_print(session, base_url, headers) -> Dict[str, Any]:
    await _post(session, base_url, headers, "/printer/print/pause")
    return {"ok": True, "message": "Print paused"}


async def resume_print(session, base_url, headers) -> Dict[str, Any]:
    await _post(session, base_url, headers, "/printer/print/resume")
    return {"ok": True, "message": "Print resumed"}


async def cancel_print(session, base_url, headers) -> Dict[str, Any]:
    await _post(session, base_url, headers, "/printer/print/cancel")
    return {"ok": True, "message": "Print cancelled"}


# ── Administration ───────────────────────────────────────────────────────────

async def emergency_stop(session, base_url, headers) -> Dict[str, Any]:
    await _post(session, base_url, headers, "/printer/emergency_stop")
    return {"ok": True, "message": "Emergency stop triggered"}


async def firmware_restart(session, base_url, headers) -> Dict[str, Any]:
    await _post(session, base_url, headers, "/printer/firmware_restart")
    return {"ok": True, "message": "Firmware restart requested"}


# ── Device Struct ─────────────────────────────────────────────────────────────
#
# NOTE: The WiZ bridge's device struct uses "type": "leds" with status
# "on"/"off" and value = brightness — a schema specific to light devices.
# There's no printer equivalent to copy, so this guesses at a reasonable
# shape. Confirm this against whatever your IRIS backend/frontend actually
# expects for non-light device types, and adjust as needed.

def build_device(printer: PrinterConfig, status: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": printer.device_id,
        "name": printer.name,
        "type": "printer",
        "status": status.get("state", "unknown"),  # standby/printing/paused/complete/error
        "value": f"{status.get('progress_pct', 0)}",
        "metadata": {
            "type": "Klipper",
            "filename": status.get("filename"),
            "hotend_temp": status.get("hotend_temp"),
            "hotend_target": status.get("hotend_target"),
            "bed_temp": status.get("bed_temp"),
            "bed_target": status.get("bed_target"),
            "fan_speed_pct": status.get("fan_speed_pct"),
            "print_duration_s": status.get("print_duration_s"),
            "total_duration_s": status.get("total_duration_s"),
            "state_message": status.get("state_message"),
        },
    }


# ── WebSocket Loop (one per printer / IRIS device) ──────────────────────────
# This is the part IRIS Home actually requires: connect, push device-update
# structs, and act on incoming commands.

async def ws_loop(printer: PrinterConfig, session: aiohttp.ClientSession, user: str):
    base_url = printer.moonraker_url.rstrip("/")
    headers = {"X-Api-Key": printer.api_key} if printer.api_key else {}

    # Bind each command function to this printer's session/base_url/headers
    functions = {
        name: partial(fn, session, base_url, headers)
        for name, fn in {
            "get_status": get_status,
            "get_info": get_info,
            "send_gcode": send_gcode,
            "home": home,
            "move": move,
            "set_extruder_temp": set_extruder_temp,
            "set_bed_temp": set_bed_temp,
            "set_fan_speed": set_fan_speed,
            "turn_off_heaters": turn_off_heaters,
            "list_files": list_files,
            "start_print": start_print,
            "pause_print": pause_print,
            "resume_print": resume_print,
            "cancel_print": cancel_print,
            "emergency_stop": emergency_stop,
            "firmware_restart": firmware_restart,
        }.items()
    }

    # Serialize HTTP calls to Moonraker (commands + polls) and writes to the socket
    printer_lock = asyncio.Lock()
    send_lock = asyncio.Lock()

    while True:
        try:
            iris_url = IRIS_URL.format(user=user, device_id=printer.device_id)
            print(f"[hub-ws:{printer.device_id}] connecting to {iris_url}")

            async with websockets.connect(iris_url) as ws:
                print(f"[hub-ws:{printer.device_id}] connected")

                last_sent: Optional[Dict[str, Any]] = None
                last_sent_at: float = 0.0

                async def push_state(force: bool = False):
                    """Poll Moonraker and send the device struct if it changed
                    (or if forced / the heartbeat interval has elapsed)."""
                    nonlocal last_sent, last_sent_at

                    try:
                        async with printer_lock:
                            status = await functions["get_status"]()
                    except Exception as e:
                        print(f"[hub-ws:{printer.device_id}] status poll failed: {e}")
                        return

                    device = build_device(printer, status)
                    heartbeat_due = (time.monotonic() - last_sent_at) >= HEARTBEAT_INTERVAL
                    if not (force or heartbeat_due or device != last_sent):
                        return

                    async with send_lock:
                        await ws.send(json.dumps(device))
                    last_sent = device
                    last_sent_at = time.monotonic()

                async def publisher():
                    """Keep the backend in sync with changes made outside Iris."""
                    while True:
                        await asyncio.sleep(STATE_POLL_INTERVAL)
                        await push_state()

                async def receiver():
                    """Handle commands from the backend."""
                    async for msg in ws:
                        try:
                            data: dict = json.loads(msg)
                        except Exception:
                            print(f"[hub-ws:{printer.device_id}] bad message: {msg!r}")
                            continue

                        print(f"[hub-ws:{printer.device_id}] recv: {data}")
                        name = data.get("cmd")
                        args = data.get("params") or {}

                        if not isinstance(args, dict):
                            try:
                                args = json.loads(args)
                            except Exception:
                                print(f"[hub-ws:{printer.device_id}] bad arguments "
                                      f"for {name}: {args}")
                                continue

                        if name not in functions:
                            print(f"[hub-ws:{printer.device_id}] unknown function: {name}")
                            continue

                        print(f"[hub-ws:{printer.device_id}] calling {name}({args})")

                        try:
                            async with printer_lock:
                                result = await functions[name](**args)
                            async with send_lock:
                                await ws.send(json.dumps({
                                    "req": data.get("req"),
                                    "result": result,
                                }))
                        except websockets.ConnectionClosed:
                            raise
                        except Exception as e:
                            print(f"[hub-ws:{printer.device_id}] error running {name}: {e}")
                            async with send_lock:
                                await ws.send(json.dumps({
                                    "req": data.get("req"),
                                    "error": str(e),
                                }))

                        # Push the state *after* the command has been applied
                        await push_state(force=True)

                # Announce initial state immediately on connect
                await push_state(force=True)

                tasks = [
                    asyncio.create_task(receiver()),
                    asyncio.create_task(publisher()),
                ]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for t in done:
                    t.result()  # re-raise any exception so we hit the reconnect handler

            print(f"[hub-ws:{printer.device_id}] connection closed, reconnecting")
            await asyncio.sleep(5)

        except Exception as e:
            print(f"[hub-ws:{printer.device_id}] error, reconnecting: {e}")
            await asyncio.sleep(5)


async def main():
    user, printers = load_config()
    if not printers:
        print("[klipper-bridge] no printers configured in config.json")
        return

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(ws_loop(printer, session, user))
            for printer in printers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
