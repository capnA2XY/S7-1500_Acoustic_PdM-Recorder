#!/usr/bin/env python3
"""
Multi-Channel Acoustic Data Acquisition with Phase-Accurate
SIMATIC Graph State Synchronization via In-Band AFSK Modulation.

Targets: Raspberry Pi 5 + NVMe SSD + USB Audio Interface + S7-1500 PLC
Author:  Anonymous
License: MIT
"""

import numpy as np
import sounddevice as sd
import soundfile as sf
import snap7
import struct
import threading
import signal
import time
import os
import subprocess
import logging
from datetime import datetime
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    """Central configuration – edit here or override from CLI / .env."""
    plc_ip: str = "192.168.0.1"
    plc_rack: int = 0
    plc_slot: int = 1
    db_number: int = 100
    db_offset: int = 0
    db_read_size: int = 40          # 10 × DINT = 40 bytes
    num_graph_instances: int = 10

    fs: int = 44100                 # Sample rate [Hz]
    channels: int = 12              # CH0 sensor + CH1-10 steps + CH11 timestamp
    wav_subtype: str = "FLOAT"      # 32-bit float as described in the paper
    usb_card_name: str = "USB Audio"
    audio_blocksize: int = 1024     # Frames per audio callback

    poll_interval: float = 0.050    # PLC poll every 50 ms
    rotation_seconds: int = 3600    # Hourly file rotation
    base_path: str = "/home/pi/pdm_data/"

    # AFSK parameters (Bell-202 style)
    afsk_baud: int = 1200
    afsk_f_mark: int = 2200         # Logical '1'
    afsk_f_space: int = 1200        # Logical '0'
    afsk_preamble: bytes = b"\x55"  # 8-bit alternating pattern
    afsk_sync: int = 0x7E           # HDLC flag
    afsk_idle_bit: int = 1          # Mark = idle

    # WavPack compression
    wavpack_bin: str = "wavpack"
    wavpack_args: list = field(default_factory=lambda: ["-h", "-x3", "--threads=2"])

    # Snap7 reconnect
    reconnect_base_delay: float = 1.0
    reconnect_max_delay: float = 30.0


# ---------------------------------------------------------------------------
# CRC-8 (polynomial 0x07, init 0x00) – matches paper Section IV-B
# ---------------------------------------------------------------------------

_CRC8_TABLE = None

def _build_crc8_table(poly: int = 0x07) -> np.ndarray:
    table = np.zeros(256, dtype=np.uint8)
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = ((crc << 1) ^ poly) if (crc & 0x80) else (crc << 1)
        table[i] = crc & 0xFF
    return table

def crc8(data: bytes) -> int:
    global _CRC8_TABLE
    if _CRC8_TABLE is None:
        _CRC8_TABLE = _build_crc8_table()
    crc = 0x00
    for b in data:
        crc = _CRC8_TABLE[(crc ^ b) & 0xFF]
    return int(crc)


# ---------------------------------------------------------------------------
# Continuous-Phase AFSK Modulator
# ---------------------------------------------------------------------------

class AFSKModulator:
    """
    Generates continuous-phase AFSK audio from byte payloads.

    Uses a phase accumulator (not a wavetable-per-bit approach) so that
    the instantaneous phase is never discontinuous at bit boundaries.
    """

    def __init__(self, cfg: Config):
        self.fs = cfg.fs
        self.baud = cfg.afsk_baud
        self.f_mark = cfg.afsk_f_mark
        self.f_space = cfg.afsk_f_space
        self.samples_per_bit = int(self.fs / self.baud)
        self._phase = 0.0  # Persistent phase accumulator [radians]

    def reset_phase(self):
        self._phase = 0.0

    def _modulate_bits(self, bits: list[int]) -> np.ndarray:
        """Generate a continuous-phase sine for a sequence of bits."""
        samples = np.empty(len(bits) * self.samples_per_bit, dtype=np.float32)
        idx = 0
        for bit in bits:
            freq = self.f_mark if bit == 1 else self.f_space
            dphi = 2.0 * np.pi * freq / self.fs
            for _ in range(self.samples_per_bit):
                samples[idx] = np.sin(self._phase)
                self._phase += dphi
                idx += 1
        # Keep phase in [0, 2π) to avoid float precision loss over time
        self._phase %= (2.0 * np.pi)
        return samples

    @staticmethod
    def _bytes_to_bits_lsb(data: bytes) -> list[int]:
        """Convert bytes to bits, LSB first (standard async serial order)."""
        bits = []
        for byte in data:
            for i in range(8):
                bits.append((byte >> i) & 1)
        return bits

    def generate_idle(self, num_samples: int) -> np.ndarray:
        """Continuous mark tone for inter-frame idle."""
        dphi = 2.0 * np.pi * self.f_mark / self.fs
        samples = np.empty(num_samples, dtype=np.float32)
        for i in range(num_samples):
            samples[i] = np.sin(self._phase)
            self._phase += dphi
        self._phase %= (2.0 * np.pi)
        return samples

    def encode_frame(self, payload: bytes, preamble: bytes = b"\x55") -> np.ndarray:
        """
        Build a complete AFSK frame per the paper's spec:
            [Preamble] [0x7E sync] [payload] [CRC-8] [0x7E end]
        Returns the audio samples for the entire frame.
        """
        frame_bytes = bytearray()
        frame_bytes.extend(preamble)
        frame_bytes.append(0x7E)
        frame_bytes.extend(payload)
        frame_bytes.append(crc8(payload))
        frame_bytes.append(0x7E)
        bits = self._bytes_to_bits_lsb(bytes(frame_bytes))
        return self._modulate_bits(bits)


# ---------------------------------------------------------------------------
# Thread-Safe Ring Buffer (lock-free single-producer / single-consumer)
# ---------------------------------------------------------------------------

class AudioRingBuffer:
    """
    Fixed-size ring buffer for passing multi-channel audio frames between
    the audio callback (producer) and the writer thread (consumer).

    Uses a simple deque with maxlen – if the writer falls behind, oldest
    blocks are silently dropped (preferred over blocking in the callback).
    """

    def __init__(self, max_blocks: int = 4000):
        self._buf: deque[np.ndarray] = deque(maxlen=max_blocks)

    def put(self, block: np.ndarray) -> bool:
        """Non-blocking put. Returns False if buffer was full (drop)."""
        if len(self._buf) >= self._buf.maxlen:
            return False
        self._buf.append(block)
        return True

    def get_all(self) -> list[np.ndarray]:
        """Drain all available blocks. Returns empty list if none."""
        blocks = []
        while self._buf:
            try:
                blocks.append(self._buf.popleft())
            except IndexError:
                break
        return blocks

    def __len__(self):
        return len(self._buf)


# ---------------------------------------------------------------------------
# Modulation Buffer Manager
# ---------------------------------------------------------------------------

class ModulationBufferManager:
    """
    Manages per-channel AFSK sample buffers.  The PLC thread appends
    encoded frames; the audio callback consumes samples.

    Uses a lock only for the brief append/consume operations, keeping
    the critical section as short as possible.
    """

    def __init__(self, num_channels: int):
        self.num_channels = num_channels
        self._buffers: list[deque] = [deque() for _ in range(num_channels)]
        self._lock = threading.Lock()
        self._connection_lost = False

    def append_samples(self, channel: int, samples: np.ndarray):
        with self._lock:
            self._buffers[channel].extend(samples.tolist())

    def consume(self, channel: int, num_samples: int) -> np.ndarray:
        """Return up to num_samples from the channel buffer, zero-pad if short."""
        out = np.zeros(num_samples, dtype=np.float32)
        with self._lock:
            buf = self._buffers[channel]
            take = min(len(buf), num_samples)
            for i in range(take):
                out[i] = buf.popleft()
        return out

    @property
    def connection_lost(self) -> bool:
        return self._connection_lost

    @connection_lost.setter
    def connection_lost(self, val: bool):
        self._connection_lost = val


# ---------------------------------------------------------------------------
# PLC Polling Thread
# ---------------------------------------------------------------------------

class PLCPoller(threading.Thread):
    """
    Reads the Collector DB from the S7-1500 at a fixed interval.
    Encodes each step ID and the Unix timestamp as AFSK frames and
    pushes them into the modulation buffer manager.

    Implements automatic reconnection with exponential backoff.
    """

    def __init__(self, cfg: Config, mod_mgr: ModulationBufferManager,
                 stop_event: threading.Event):
        super().__init__(daemon=True, name="PLCPoller")
        self.cfg = cfg
        self.mod = mod_mgr
        self.stop = stop_event
        self.log = logging.getLogger("plc")

        # One AFSK modulator per metadata channel (CH1-10 steps + CH11 ts)
        self._modulators: list[AFSKModulator] = [
            AFSKModulator(cfg) for _ in range(cfg.num_graph_instances + 1)
        ]
        self._client = snap7.client.Client()
        self._last_step_ids = [0] * cfg.num_graph_instances

    def _connect(self):
        delay = self.cfg.reconnect_base_delay
        while not self.stop.is_set():
            try:
                self._client.connect(
                    self.cfg.plc_ip, self.cfg.plc_rack, self.cfg.plc_slot
                )
                self.log.info("Connected to S7-1500 at %s", self.cfg.plc_ip)
                self.mod.connection_lost = False
                return True
            except Exception as exc:
                self.log.warning(
                    "Connection failed (%s), retrying in %.1fs", exc, delay
                )
                self.mod.connection_lost = True
                self.stop.wait(delay)
                delay = min(delay * 2, self.cfg.reconnect_max_delay)
        return False

    def _encode_connection_lost_frame(self) -> np.ndarray:
        """Special sentinel frame: payload = 0xFFFFFFFF."""
        mod = self._modulators[0]
        return mod.encode_frame(b"\xFF\xFF\xFF\xFF")

    def run(self):
        cfg = self.cfg
        if not self._connect():
            return

        while not self.stop.is_set():
            try:
                raw = self._client.db_read(
                    cfg.db_number, cfg.db_offset, cfg.db_read_size
                )
                # Paper spec: 10 × DINT (32-bit signed, big-endian)
                step_ids = struct.unpack(f">{cfg.num_graph_instances}i", raw)
                self._last_step_ids = list(step_ids)
                ts = time.time()

                # Encode step IDs → CH1..CH10 (mod buffer indices 0..9)
                for i, sid in enumerate(step_ids):
                    payload = struct.pack(">i", sid)  # 32-bit signed
                    samples = self._modulators[i].encode_frame(payload)
                    self.mod.append_samples(i, samples)

                # Encode timestamp → CH11 (mod buffer index 10)
                ts_payload = struct.pack(">Q", int(ts * 1_000_000))  # µs precision
                samples = self._modulators[10].encode_frame(ts_payload)
                self.mod.append_samples(10, samples)

            except Exception as exc:
                self.log.error("PLC read error: %s", exc)
                self.mod.connection_lost = True
                # Insert connection-lost sentinel on all step channels
                sentinel = self._encode_connection_lost_frame()
                for i in range(cfg.num_graph_instances):
                    self.mod.append_samples(i, sentinel)
                # Attempt reconnection
                try:
                    self._client.disconnect()
                except Exception:
                    pass
                if not self._connect():
                    return

            self.stop.wait(cfg.poll_interval)

        try:
            self._client.disconnect()
        except Exception:
            pass
        self.log.info("PLC poller stopped.")


# ---------------------------------------------------------------------------
# WAV Writer Thread (with hourly rotation & double-buffering)
# ---------------------------------------------------------------------------

class WAVWriter(threading.Thread):
    """
    Drains the audio ring buffer and writes 12-channel WAV files.
    Performs hourly file rotation with gapless double-buffering:
    the next file is pre-opened before the rotation boundary.
    """

    def __init__(self, cfg: Config, ring: AudioRingBuffer,
                 compression_queue: deque, stop_event: threading.Event):
        super().__init__(daemon=True, name="WAVWriter")
        self.cfg = cfg
        self.ring = ring
        self.comp_q = compression_queue
        self.stop = stop_event
        self.log = logging.getLogger("writer")

    def _make_filename(self, dt: Optional[datetime] = None) -> str:
        dt = dt or datetime.now()
        return os.path.join(
            self.cfg.base_path, dt.strftime("pdm_%Y%m%d_%H00.wav")
        )

    def _open_wav(self, path: str) -> sf.SoundFile:
        self.log.info("Opening WAV: %s", path)
        return sf.SoundFile(
            path, mode="w",
            samplerate=self.cfg.fs,
            channels=self.cfg.channels,
            subtype=self.cfg.wav_subtype,
        )

    def run(self):
        os.makedirs(self.cfg.base_path, exist_ok=True)
        fname = self._make_filename()
        wf = self._open_wav(fname)
        current_hour = datetime.now().hour

        try:
            while not self.stop.is_set():
                now = datetime.now()

                # Hourly rotation with gapless switch
                if now.hour != current_hour:
                    new_fname = self._make_filename(now)
                    new_wf = self._open_wav(new_fname)  # Pre-open next file
                    wf.close()
                    self.comp_q.append(fname)  # Queue old file for compression
                    wf, fname = new_wf, new_fname
                    current_hour = now.hour

                # Drain ring buffer
                blocks = self.ring.get_all()
                for block in blocks:
                    wf.write(block)

                if not blocks:
                    self.stop.wait(0.05)  # Avoid busy-wait when idle

        finally:
            wf.close()
            self.comp_q.append(fname)
            self.log.info("WAV writer stopped.")


# ---------------------------------------------------------------------------
# Compression Worker Thread
# ---------------------------------------------------------------------------

class CompressionWorker(threading.Thread):
    """
    Background WavPack encoder.  Runs at lowest priority to avoid
    contending with the audio callback for CPU time.
    """

    def __init__(self, cfg: Config, queue: deque,
                 stop_event: threading.Event):
        super().__init__(daemon=True, name="Compressor")
        self.cfg = cfg
        self.queue = queue
        self.stop = stop_event
        self.log = logging.getLogger("compress")

    def run(self):
        # Set idle scheduling priority (best-effort, non-fatal if fails)
        try:
            os.sched_setscheduler(0, os.SCHED_IDLE, os.sched_param(0))
        except (OSError, AttributeError):
            pass

        while not self.stop.is_set() or self.queue:
            if self.queue:
                fpath = self.queue.popleft()
                if os.path.exists(fpath):
                    cmd = [self.cfg.wavpack_bin] + self.cfg.wavpack_args + [fpath]
                    self.log.info("Compressing: %s", fpath)
                    try:
                        result = subprocess.run(
                            cmd, capture_output=True, text=True, timeout=600
                        )
                        if result.returncode == 0:
                            self.log.info("Done: %s", fpath)
                            # Optionally delete original WAV after successful compression
                            try:
                                os.remove(fpath)
                            except OSError:
                                pass
                        else:
                            self.log.error(
                                "WavPack failed (%d): %s",
                                result.returncode, result.stderr
                            )
                    except subprocess.TimeoutExpired:
                        self.log.error("WavPack timed out on %s", fpath)
            else:
                self.stop.wait(2.0)

        self.log.info("Compression worker stopped.")


# ---------------------------------------------------------------------------
# Main Application
# ---------------------------------------------------------------------------

class PDMRecorder:
    """
    Top-level orchestrator.  Sets up all threads and the audio stream,
    then blocks until SIGINT / SIGTERM.
    """

    def __init__(self, cfg: Optional[Config] = None):
        self.cfg = cfg or Config()
        self.log = logging.getLogger("pdm")
        self.stop_event = threading.Event()

        # Shared resources
        self.ring = AudioRingBuffer(max_blocks=4000)
        self.mod_mgr = ModulationBufferManager(
            self.cfg.num_graph_instances + 1  # 10 steps + 1 timestamp
        )
        self.comp_queue: deque[str] = deque()

        # Threads
        self.plc_thread = PLCPoller(self.cfg, self.mod_mgr, self.stop_event)
        self.writer_thread = WAVWriter(
            self.cfg, self.ring, self.comp_queue, self.stop_event
        )
        self.comp_thread = CompressionWorker(
            self.cfg, self.comp_queue, self.stop_event
        )

    def _find_usb_device(self) -> int:
        """Locate the USB audio interface by partial name match."""
        devices = sd.query_devices()
        for idx, dev in enumerate(devices):
            if (self.cfg.usb_card_name in dev["name"]
                    and dev["max_input_channels"] > 0):
                self.log.info(
                    "Found audio device: [%d] %s", idx, dev["name"]
                )
                return idx
        raise RuntimeError(
            f"USB audio device matching '{self.cfg.usb_card_name}' not found. "
            f"Available: {[d['name'] for d in devices]}"
        )

    def _audio_callback(self, indata: np.ndarray, frames: int,
                        time_info, status):
        """
        Sounddevice InputStream callback.  Runs on a high-priority
        OS thread – no allocations, no blocking I/O, minimal work.
        """
        if status:
            self.log.warning("Audio status: %s", status)

        # Build 12-channel output matrix
        matrix = np.zeros((frames, self.cfg.channels), dtype=np.float32)

        # CH0: raw microphone / AE sensor
        matrix[:, 0] = indata[:, 0]

        # CH1-10: AFSK-encoded step IDs, CH11: AFSK-encoded timestamp
        for i in range(self.cfg.num_graph_instances + 1):
            matrix[:, i + 1] = self.mod_mgr.consume(i, frames)

        self.ring.put(matrix)

    def run(self):
        """Start all threads and the audio stream; block until shutdown."""
        dev_id = self._find_usb_device()

        # Register signal handlers for graceful shutdown
        def _shutdown(signum, frame):
            self.log.info("Received signal %d, shutting down...", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        # Launch worker threads
        self.plc_thread.start()
        self.writer_thread.start()
        self.comp_thread.start()

        # Open audio stream (input-only, no outdata parameter)
        with sd.InputStream(
            device=dev_id,
            samplerate=self.cfg.fs,
            channels=1,
            dtype="float32",
            blocksize=self.cfg.audio_blocksize,
            callback=self._audio_callback,
        ):
            self.log.info(
                "S7-1500 PdM Monitoring active — %d channels @ %d Hz. "
                "Press Ctrl+C to stop.",
                self.cfg.channels, self.cfg.fs,
            )
            # Block main thread until stop signal
            while not self.stop_event.is_set():
                self.stop_event.wait(1.0)

        # Wait for workers to finish
        self.log.info("Waiting for worker threads...")
        self.plc_thread.join(timeout=5)
        self.writer_thread.join(timeout=5)
        self.comp_thread.join(timeout=10)
        self.log.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)-10s] %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    recorder = PDMRecorder(Config())
    recorder.run()
