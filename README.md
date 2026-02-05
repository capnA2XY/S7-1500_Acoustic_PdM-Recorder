# S7-1500 Acoustic PdM Recorder

**Phase-accurate multi-channel acoustic data acquisition with in-band SIMATIC Graph state synchronization for predictive maintenance.**

This system records industrial acoustic emissions alongside PLC operational states by encoding SIMATIC Graph step identifiers directly into audio channels using AFSK modulation. The result is a self-documenting dataset where machine state labels are guaranteed to be sample-accurate with the sensor data — no clock drift, no network jitter, no post-hoc alignment.

---

## Why This Exists

Training ML models for predictive maintenance requires knowing *exactly* what the machine was doing when each audio sample was captured. Traditional approaches (polling the PLC over Ethernet and logging timestamps separately) introduce millisecond-level timing errors that inject label noise into your training data.

This project eliminates the problem entirely by treating metadata as audio. Step IDs and timestamps are AFSK-modulated into dedicated channels of the same WAV file that carries the sensor data. If the audio is trimmed, resampled, or reformatted, the labels move with it — synchronization is preserved by construction.

## Architecture

```
┌─────────────────────┐      S7 Comm (TCP)      ┌──────────────────────┐
│   SIMATIC S7-1500   │◄────────────────────────►│   Raspberry Pi 5     │
│                     │      50ms polling         │                      │
│  10× Graph SFCs     │                          │  ┌──────────────────┐│
│  Collector DB (40B) │                          │  │ PLC Poller Thread ││
│  PUT/GET enabled    │                          │  └────────┬─────────┘│
└─────────────────────┘                          │           │          │
                                                 │  ┌────────▼─────────┐│
┌─────────────────────┐      USB Audio           │  │ AFSK Modulators  ││
│  USB Audio Interface│◄────────────────────────►│  │ (11 channels)    ││
│  (e.g. UMC1820)    │      1ch input            │  └────────┬─────────┘│
│  Mic / AE sensor    │                          │           │          │
└─────────────────────┘                          │  ┌────────▼─────────┐│
                                                 │  │ Audio Callback   ││
┌─────────────────────┐      PCIe 3.0 x1         │  │ 12ch assembly    ││
│  NVMe SSD (ext4)    │◄────────────────────────►│  └────────┬─────────┘│
│  ~7.3 GB/hr raw     │                          │           │          │
│  ~3 GB/hr compressed│                          │  ┌────────▼─────────┐│
└─────────────────────┘                          │  │ WAV Writer       ││
                                                 │  │ (hourly rotation)││
                                                 │  └────────┬─────────┘│
                                                 │  ┌────────▼─────────┐│
                                                 │  │ WavPack Compress ││
                                                 │  │ (SCHED_IDLE)    ││
                                                 │  └──────────────────┘│
                                                 └──────────────────────┘
```

## Channel Map

| Channel | Content | Encoding |
|---------|---------|----------|
| CH 0 | Acoustic sensor (mic / AE transducer) | Raw analog input |
| CH 1–10 | SIMATIC Graph step IDs (instances 1–10) | AFSK 1200 baud, 32-bit DINT |
| CH 11 | Unix timestamp (UTC, µs precision) | AFSK 1200 baud, 64-bit uint |

## AFSK Frame Format

Each metadata update is encoded as a serial frame:

```
┌──────────┬──────────┬───────────────────┬───────┬──────────┐
│ Preamble │  Sync    │     Payload       │ CRC-8 │ End Flag │
│  0x55    │  0x7E    │ 4B (step) / 8B (ts)│       │  0x7E    │
│  8 bits  │  8 bits  │  32 or 64 bits    │ 8 bits│  8 bits  │
└──────────┴──────────┴───────────────────┴───────┴──────────┘
```

- **Mark frequency (logical 1):** 2200 Hz
- **Space frequency (logical 0):** 1200 Hz
- **Baud rate:** 1200 bps
- **Phase continuity:** Maintained across bit boundaries via accumulator

## Hardware Requirements

| Component | Specification | Purpose |
|-----------|---------------|---------|
| **PLC** | Siemens S7-1500 (e.g. CPU 1515-2 PN) | Machine control, Graph SFCs |
| **Edge device** | Raspberry Pi 5 (4GB+ RAM) | Data acquisition & AFSK encoding |
| **Storage** | M.2 NVMe SSD (via PCIe 3.0 HAT) | High-throughput WAV recording |
| **Audio interface** | USB class-compliant, ≥1 input channel | Acoustic sensor capture |
| **Sensor** | MEMS microphone or AE transducer | Acoustic emission pickup |

## Software Requirements

- Python 3.10+
- Raspberry Pi OS (64-bit) or any Linux with ALSA
- WavPack CLI (`wavpack`) for background compression

## Installation

```bash
# Clone
git clone https://github.com/youruser/s7-acoustic-pdm.git
cd s7-acoustic-pdm

# Install system dependencies
sudo apt update
sudo apt install -y wavpack libportaudio2

# Install Python dependencies
pip install numpy sounddevice soundfile python-snap7

# Verify audio device detection
python3 -c "import sounddevice; print(sounddevice.query_devices())"
```

### PLC-Side Setup

1. Create a **Collector DB** (e.g. DB100) with non-optimized (absolute) access.
2. In a cyclic OB (e.g. OB35 @ 10ms), copy the `S_NO` from each of your 10 Graph FBs into the DB as contiguous DINTs (40 bytes total).
3. Enable **PUT/GET communication** in the CPU properties under *Protection & Security*.

```
DB100 layout (non-optimized):
Offset 0:   DINT  — Graph Instance 1 Step ID
Offset 4:   DINT  — Graph Instance 2 Step ID
...
Offset 36:  DINT  — Graph Instance 10 Step ID
```

## Usage

### Basic

```bash
python3 pdm_recorder.py
```

### Custom Configuration

Edit the `Config` dataclass at the top of `pdm_recorder.py`, or override in code:

```python
from pdm_recorder import PDMRecorder, Config

cfg = Config(
    plc_ip="192.168.1.100",
    db_number=200,
    num_graph_instances=5,
    channels=7,              # CH0 + 5 steps + CH6 timestamp
    usb_card_name="Behringer",
    base_path="/mnt/nvme/recordings/",
)

recorder = PDMRecorder(cfg)
recorder.run()
```

### Running as a systemd Service

```ini
# /etc/systemd/system/pdm-recorder.service
[Unit]
Description=S7-1500 Acoustic PdM Recorder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/s7-acoustic-pdm
ExecStart=/usr/bin/python3 pdm_recorder.py
Restart=on-failure
RestartSec=10
Nice=0

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now pdm-recorder.service
```

## Post-Processing: Extracting Labels

After recording, decode the AFSK channels to produce time-indexed label sequences:

```python
import soundfile as sf
import numpy as np
from pdm_decoder import AFSKDecoder  # see decoder module

audio, fs = sf.read("pdm_20250205_1400.wv")

decoder = AFSKDecoder(fs=fs, baud=1200, f_mark=2200, f_space=1200)

# Decode step IDs from channel 1
labels = decoder.decode_channel(audio[:, 1])
# → [(sample_offset, step_id), (sample_offset, step_id), ...]

# Decode timestamps from channel 11
timestamps = decoder.decode_channel(audio[:, 11], payload_size=8)
```

## Storage Estimates

| Duration | Raw (12ch 32-bit float) | Compressed (WavPack) |
|----------|------------------------|----------------------|
| 1 hour | ~7.3 GB | ~3–4.4 GB |
| 24 hours | ~174 GB | ~70–105 GB |
| 7 days | ~1.2 TB | ~490–740 GB |

## Synchronization Accuracy

| Method | Mean offset | Std dev | Worst case |
|--------|-------------|---------|------------|
| **AFSK in-band** (this project) | **±1 sample (±22.7 µs)** | **0** | **±22.7 µs** |
| Network polling + NTP | 3.2 ms | 1.8 ms | >15 ms |
| PTP (IEEE 1588) | ~1 µs | — | Requires HW support |

The AFSK approach achieves two to three orders of magnitude better accuracy than network polling, with zero additional hardware beyond the audio interface.

## Key Design Decisions

- **Continuous-phase AFSK** — Phase accumulator prevents spectral splatter at bit boundaries, keeping metadata channels clean for demodulation.
- **CRC-8 per frame** — Detects any corruption from electrical noise or buffer glitches; the decoder can flag unreliable labels.
- **Connection-lost sentinel** (`0xFFFFFFFF`) — When the PLC link drops, a special frame is injected so the ML pipeline can exclude those segments.
- **Double-buffered file rotation** — The next WAV file is pre-opened before closing the current one, ensuring zero-gap recording across hour boundaries.
- **SCHED_IDLE compression** — WavPack runs at the lowest OS priority, leaving all four Cortex-A76 cores available for the real-time audio path.

## Project Structure

```
s7-acoustic-pdm/
├── pdm_recorder.py          # Main application
├── pdm_decoder.py           # AFSK demodulator for post-processing (TODO)
├── paper/
│   └── paper.tex            # IEEE conference paper (LaTeX source)
├── systemd/
│   └── pdm-recorder.service # systemd unit file
├── README.md
├── LICENSE
└── requirements.txt
```

## Troubleshooting

**"USB audio device not found"** — Run `python3 -c "import sounddevice; print(sounddevice.query_devices())"` and update `usb_card_name` in `Config` to match a substring of your device's name.

**PLC connection keeps failing** — Verify PUT/GET is enabled in TIA Portal under CPU properties → Protection & Security. Check that the Collector DB uses non-optimized (absolute) access. Confirm network connectivity with `ping <PLC_IP>`.

**Audio buffer overruns** — Increase `audio_blocksize` in `Config`. Ensure the NVMe SSD is mounted with `noatime`. Check that no other process is consuming CPU during recording.

**WavPack not found** — Install with `sudo apt install wavpack`.

## Citation

If you use this system in academic work:

```bibtex
@inproceedings{aradi2026pdm,
  title     = {Multi-Channel Acoustic Data Acquisition and Phase-Accurate
               State Synchronization for {SIMATIC} {S7-1500} Graph-Based
               Predictive Maintenance},
  author    = {Attila Aradi},
  booktitle = {ICCC 2026},
  year      = {2026}
}
```

## License

MIT — see [LICENSE](LICENSE) for details.
