#!/usr/bin/env python3
"""
Klipper / Moonraker Bridge — LLM-ready tool interface via WebSocket

Bridges one or more Klipper-based 3D printers (each running Moonraker) to
IRIS Home, following the same shape as the WiZ light bridge:
    - a Controller class with async methods that all return a Result
    - a `functions` dict used to dispatch incoming backend commands
    - a per-device WebSocket loop that polls state, pushes changes, and
      relays commands back to the printer

Unlike WiZ bulbs, Klipper printers aren't discoverable via broadcast — each
one is a Moonraker instance at a known host:port. So instead of a discovery
step, this bridge reads a list of printers from config.json and spins up one
WebSocket connection (one IRIS device) per printer. Adding another
Klipper-based printer later (Ender 3, Voron, Prusa w/ Klipper, etc.) is just
another entry in that list — no code changes needed.
"""

import asyncio
import json
import time
import aiohttp
import websockets

from typing import Any, Dict, List, Optional
from dataclasses import dataclass
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


# ── Data Models ──────────────────────────────────────────────────────────────

@dataclass
class Result:
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
        }


@dataclass
class PrinterConfig:
    device_id: str          # unique id used as the IRIS device id
    name: str                # display name, e.g. "Ender 3 V3 SE"
    moonraker_url: str      # e.g. "http://localhost:7125"
    api_key: Optional[str] = None  # only needed if Moonraker auth is enabled


# ── Klipper Controller (one instance per printer, returns Results) ─────────

class KlipperController:
    """
    Wraps one Moonraker instance's HTTP API. See:
    https://moonraker.readthedocs.io/en/latest/external_api/printer/
    """

    def __init__(self, printer: PrinterConfig, session: aiohttp.ClientSession):
        self.printer = printer
        self.session = session
        self.base_url = printer.moonraker_url.rstrip("/")
        self.headers = {"X-Api-Key": printer.api_key} if printer.api_key else {}
        self.timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)

    # ── Low-level HTTP helpers ────────────────────────────────────────────

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        async with self.session.get(
            f"{self.base_url}{path}", params=params,
            headers=self.headers, timeout=self.timeout,
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
            return body.get("result", body)

    async def _post(self, path: str, json_body: Optional[Dict[str, Any]] = None,
                     params: Optional[Dict[str, Any]] = None) -> Any:
        async with self.session.post(
            f"{self.base_url}{path}", json=json_body, params=params,
            headers=self.headers, timeout=self.timeout,
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
            return body.get("result", body)

    # ── Status ──────────────────────────────────────────────────────────────

    async def get_status(self) -> Result:
        try:
            # GET with a bare query string requests all attributes of each
            # listed object, e.g. ?print_stats&toolhead&extruder
            query = "&".join(STATUS_OBJECTS)
            result = await self._get(f"/printer/objects/query?{query}")
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

            return Result(
                success=True,
                message="Status retrieved",
                data={
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
                },
            )
        except Exception as e:
            return Result(success=False, message=f"Error getting status: {e}")

    async def get_info(self) -> Result:
        """Klippy host info: hostname, software version, ready/error/etc."""
        try:
            info = await self._get("/printer/info")
            return Result(
                success=True,
                message="Info retrieved",
                data={
                    "state": info.get("state"),
                    "state_message": info.get("state_message"),
                    "hostname": info.get("hostname"),
                    "software_version": info.get("software_version"),
                },
            )
        except Exception as e:
            return Result(success=False, message=f"Error getting info: {e}")

    # ── GCode / Motion ────────────────────────────────────────────────────

    async def send_gcode(self, script: str) -> Result:
        if not script or not isinstance(script, str):
            return Result(success=False, message="script must be a non-empty string")
        try:
            await self._post("/printer/gcode/script", json_body={"script": script})
            return Result(success=True, message=f"Ran: {script}")
        except Exception as e:
            return Result(success=False, message=f"GCode error: {e}")

    async def home(self, axes: Optional[str] = None) -> Result:
        """axes: e.g. 'XYZ', 'XY', 'Z'. Omit/empty to home all axes."""
        letters = "".join(ch for ch in (axes or "").upper() if ch in "XYZ")
        script = f"G28 {' '.join(letters)}".strip() if letters else "G28"
        return await self.send_gcode(script)

    async def move(
        self,
        x: Optional[float] = None,
        y: Optional[float] = None,
        z: Optional[float] = None,
        e: Optional[float] = None,
        feedrate: float = 1500,
        relative: bool = True,
    ) -> Result:
        """Jog the toolhead. Relative moves (the default) are safest for
        manual jogging since they don't depend on the current position."""
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
            return Result(success=False, message="Provide at least one of x/y/z/e")

        mode = "G91" if relative else "G90"
        script = f"{mode}\nG1 {' '.join(parts)} F{feedrate}"
        if relative:
            script += "\nG90"  # restore absolute mode afterward
        return await self.send_gcode(script)

    # ── Temperature / Fan ─────────────────────────────────────────────────

    async def set_extruder_temp(self, temp: float, tool: int = 0) -> Result:
        return await self.send_gcode(f"M104 T{tool} S{temp}")

    async def set_bed_temp(self, temp: float) -> Result:
        return await self.send_gcode(f"M140 S{temp}")

    async def set_fan_speed(self, percent: float) -> Result:
        if not 0 <= percent <= 100:
            return Result(success=False, message="percent must be 0-100")
        return await self.send_gcode(f"M106 S{round(percent / 100 * 255)}")

    async def turn_off_heaters(self) -> Result:
        return await self.send_gcode("TURN_OFF_HEATERS")

    # ── Files ───────────────────────────────────────────────────────────────

    async def list_files(self) -> Result:
        try:
            files = await self._get("/server/files/list")
            return Result(
                success=True,
                message=f"Found {len(files)} file(s)",
                data={"files": [
                    {"path": f.get("path"), "size": f.get("size"),
                     "modified": f.get("modified")}
                    for f in files
                ]},
            )
        except Exception as e:
            return Result(success=False, message=f"Error listing files: {e}")

    # ── Print Job Management ──────────────────────────────────────────────

    async def start_print(self, filename: str) -> Result:
        if not filename:
            return Result(success=False, message="filename is required")
        try:
            await self._post("/printer/print/start", params={"filename": filename})
            return Result(success=True, message=f"Started print: {filename}")
        except Exception as e:
            return Result(success=False, message=f"Error starting print: {e}")

    async def pause_print(self) -> Result:
        try:
            await self._post("/printer/print/pause")
            return Result(success=True, message="Print paused")
        except Exception as e:
            return Result(success=False, message=f"Error pausing: {e}")

    async def resume_print(self) -> Result:
        try:
            await self._post("/printer/print/resume")
            return Result(success=True, message="Print resumed")
        except Exception as e:
            return Result(success=False, message=f"Error resuming: {e}")

    async def cancel_print(self) -> Result:
        try:
            await self._post("/printer/print/cancel")
            return Result(success=True, message="Print cancelled")
        except Exception as e:
            return Result(success=False, message=f"Error cancelling: {e}")

    # ── Administration ────────────────────────────────────────────────────

    async def emergency_stop(self) -> Result:
        try:
            await self._post("/printer/emergency_stop")
            return Result(success=True, message="Emergency stop triggered")
        except Exception as e:
            return Result(success=False, message=f"Error: {e}")

    async def firmware_restart(self) -> Result:
        try:
            await self._post("/printer/firmware_restart")
            return Result(success=True, message="Firmware restart requested")
        except Exception as e:
            return Result(success=False, message=f"Error: {e}")


# ── Config Loader ────────────────────────────────────────────────────────────

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


# ── Device Struct ─────────────────────────────────────────────────────────────
#
# NOTE: This mirrors the shape of the WiZ bridge's device struct
# (id/name/type/status/value/metadata), guessing at reasonable values for a
# printer. Confirm this against whatever your IRIS backend/frontend actually
# expects for non-light device types, and adjust the "type"/"status"/"value"
# mapping below if needed.

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

async def ws_loop(printer: PrinterConfig, session: aiohttp.ClientSession, user: str):
    controller = KlipperController(printer, session)

    # Serialize HTTP calls to Moonraker (commands + polls) and writes to the socket
    printer_lock = asyncio.Lock()
    send_lock = asyncio.Lock()

    functions = {
        "get_status": controller.get_status,
        "get_info": controller.get_info,
        "send_gcode": controller.send_gcode,
        "home": controller.home,
        "move": controller.move,
        "set_extruder_temp": controller.set_extruder_temp,
        "set_bed_temp": controller.set_bed_temp,
        "set_fan_speed": controller.set_fan_speed,
        "turn_off_heaters": controller.turn_off_heaters,
        "list_files": controller.list_files,
        "start_print": controller.start_print,
        "pause_print": controller.pause_print,
        "resume_print": controller.resume_print,
        "cancel_print": controller.cancel_print,
        "emergency_stop": controller.emergency_stop,
        "firmware_restart": controller.firmware_restart,
    }

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

                    async with printer_lock:
                        status_result = await controller.get_status()

                    if not status_result.success:
                        print(f"[hub-ws:{printer.device_id}] status poll failed: "
                              f"{status_result.message}")
                        return

                    device = build_device(printer, status_result.data or {})
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

                        fn = functions[name]
                        print(f"[hub-ws:{printer.device_id}] calling {name}({args})")

                        try:
                            async with printer_lock:
                                result: Result = await fn(**args)
                            async with send_lock:
                                await ws.send(json.dumps({
                                    "req": data.get("req"),
                                    "result": result.to_dict(),
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
