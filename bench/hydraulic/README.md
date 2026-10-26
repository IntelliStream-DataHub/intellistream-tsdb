# Real sensor data: a hydraulic test rig

Most of tsdb's compression numbers come from made-up sensor data (`tools/datasets.h`). This
directory measures the same thing on real measurements, so anyone can check that the codecs
work as well on data from a real machine.

## The data

The data set is **"Condition monitoring of hydraulic systems"** from the UCI Machine Learning
Repository:
<https://archive.ics.uci.edu/dataset/447/condition+monitoring+of+hydraulic+systems>

A hydraulic test rig (pumps, valves, a cooler, an accumulator, an oil tank) ran the same
60-second work cycle 2205 times in a row, about 37 hours, while 17 sensors recorded it:

| sensors | what they measure | unit | samples per second | values per sensor |
|---|---|---|---:|---:|
| PS1 - PS6 | oil pressure | bar | 100 | 13,230,000 |
| EPS1 | motor power | W | 100 | 13,230,000 |
| FS1, FS2 | oil flow | l/min | 10 | 1,323,000 |
| TS1 - TS4 | temperature | °C | 1 | 132,300 |
| VS1 | vibration | mm/s | 1 | 132,300 |
| CE | cooling efficiency (computed) | % | 1 | 132,300 |
| CP | cooling power (computed) | kW | 1 | 132,300 |
| SE | efficiency factor (computed) | % | 1 | 132,300 |

The values are written as decimal text with up to 3 decimals (PS1: 2, EPS1: 1).

**Licence and credit.** The data is published under the Creative Commons Attribution 4.0
licence (CC BY 4.0). It was made by Nikolai Helwig, Eliseo Pignanelli and Andreas Schütze at
ZeMA gGmbH (Saarbrücken), and described in: N. Helwig, E. Pignanelli, A. Schütze, "Condition
Monitoring of a Complex Hydraulic System Using Multivariate Statistics", Proc. I2MTC-2015, IEEE
International Instrumentation and Measurement Technology Conference, Pisa, Italy, 2015,
doi:10.1109/I2MTC.2015.7151267. If you publish numbers measured on it, give this credit.

## What you need

- this repository, and what `make` needs to build it (a C compiler, libzstd)
- `curl`, `unzip`, `sha256sum`, Python 3 (no extra packages)
- about **2.7 GB** of free disk: 77 MB download, 556 MB unpacked text, 2.0 GB of converted values

The steps below put everything in `/var/tmp/hydraulic`; any directory works.
Run them from the top directory of this repository.

## Steps

**1. Download** (77 MB; a few seconds to a minute, depending on your connection):

```sh
bench/hydraulic/get.sh /var/tmp/hydraulic
```

It checks the SHA-256 checksum of the file, so you know you have exactly the data these numbers
came from, and unpacks the text files (one per sensor, `PS1.txt` and so on).

**2. Convert** (about 20 seconds):

```sh
python3 -I bench/hydraulic/convert.py /var/tmp/hydraulic /var/tmp/hydraulic/series
```

This turns every sensor into one series of numbers in time order and writes it twice: as
float64 (`PS1.f64`, 8 bytes per value) and as float32 (`PS1.f32`, 4 bytes per value), each value
the number closest to the decimal in the text. For the six pressures it also writes the values
converted from bar to psi (`PS1_psi.f64`, `PS1_psi.f32`): bar × 14.503773773, not rounded. That is
what a collector that converts units produces, and such numbers have many more digits, so they
are much harder to compress.

**3. Run** (about 2 minutes, the first build included):

```sh
bench/hydraulic/run.sh /var/tmp/hydraulic/series
```

It builds `build/codec-bench` and `build/type-bench`, compresses every series in blocks of 8192
values the way tsdb stores them, decodes each block again to check that every value comes back
exactly, and prints the table.

**4. Read the table.** One row per sensor and value type:

| column | meaning |
|---|---|
| series | the sensor; "psi" is the pressure converted to psi |
| type | float32 or float64 |
| values | how many values the series has |
| tsdb, bits/value | the size in tsdb with its default settings, in bits per value. Smaller is better. Uncompressed, a float32 takes 32 bits and a float64 64 |
| Gorilla + ZSTD | the Gorilla codec (used by many time series databases), then ZSTD level 1 |
| Chimp + ZSTD | the Chimp codec, then ZSTD (float32 only) |
| byte split + ZSTD | the bytes of each value regrouped, then ZSTD (what Parquet's BYTE_STREAM_SPLIT does) |

Each other codec shows its bits per value and, in brackets, its size compared to tsdb: 1.0x is the
same size as tsdb, 2.9x is 2.9 times larger. You can also call the tools on one file yourself:
`build/codec-bench --file /var/tmp/hydraulic/series/TS1.f32` lists every float32 codec, and
`build/type-bench --file /var/tmp/hydraulic/series/TS1.f64 float64` the float64 ones.

## Results

Measured on commit c821848 (tsdb's codecs do not depend on the CPU, so you should get the same
numbers on any x86-64 machine).

| series | type | values | tsdb, bits/value | Gorilla + ZSTD | Chimp + ZSTD | byte split + ZSTD |
|---|---|---:|---:|---:|---:|---:|
| PS1 | float32 | 13230000 | **6.69** | 19.61 (2.9x) | 16.31 (2.4x) | 13.28 (2.0x) |
| PS1 | float64 | 13230000 | **6.70** | 31.10 (4.6x) | - | 35.20 (5.3x) |
| PS1 psi | float32 | 13230000 | **15.52** | 20.00 (1.3x) | 16.58 (1.1x) | 15.73 (1.0x) |
| PS1 psi | float64 | 13230000 | **43.63** | 43.73 (1.0x) | - | 46.96 (1.1x) |
| PS2 | float32 | 13230000 | **7.56** | 19.32 (2.6x) | 15.69 (2.1x) | 12.55 (1.7x) |
| PS2 | float64 | 13230000 | **7.73** | 29.30 (3.8x) | - | 31.67 (4.1x) |
| PS2 psi | float32 | 13230000 | **12.89** | 20.07 (1.6x) | 16.16 (1.3x) | 14.75 (1.1x) |
| PS2 psi | float64 | 13230000 | **41.39** | 43.48 (1.1x) | - | 41.45 (1.0x) |
| PS3 | float32 | 13230000 | **7.83** | 20.04 (2.6x) | 18.55 (2.4x) | 15.68 (2.0x) |
| PS3 | float64 | 13230000 | **7.83** | 22.79 (2.9x) | - | 35.99 (4.6x) |
| PS3 psi | float32 | 13230000 | **14.51** | 21.22 (1.5x) | 19.32 (1.3x) | 17.28 (1.2x) |
| PS3 psi | float64 | 13230000 | **30.21** | 30.23 (1.0x) | - | 39.59 (1.3x) |
| PS4 | float32 | 13230000 | **2.50** | 7.29 (2.9x) | 6.78 (2.7x) | 5.86 (2.3x) |
| PS4 | float64 | 13230000 | **2.57** | 15.82 (6.2x) | - | 15.09 (5.9x) |
| PS4 psi | float32 | 13230000 | **5.21** | 7.15 (1.4x) | 6.74 (1.3x) | 5.85 (1.1x) |
| PS4 psi | float64 | 13230000 | **15.85** | 16.43 (1.0x) | - | 15.85 (1.0x) |
| PS5 | float32 | 13230000 | **6.13** | 20.07 (3.3x) | 19.11 (3.1x) | 14.98 (2.4x) |
| PS5 | float64 | 13230000 | **6.13** | 43.60 (7.1x) | - | 44.02 (7.2x) |
| PS5 psi | float32 | 13230000 | **12.52** | 20.25 (1.6x) | 19.65 (1.6x) | 14.85 (1.2x) |
| PS5 psi | float64 | 13230000 | **44.47** | 44.63 (1.0x) | - | 45.75 (1.0x) |
| PS6 | float32 | 13230000 | **6.15** | 20.09 (3.3x) | 19.11 (3.1x) | 15.12 (2.5x) |
| PS6 | float64 | 13230000 | **6.15** | 44.09 (7.2x) | - | 44.03 (7.2x) |
| PS6 psi | float32 | 13230000 | **12.77** | 19.88 (1.6x) | 19.59 (1.5x) | 14.90 (1.2x) |
| PS6 psi | float64 | 13230000 | **44.73** | 44.93 (1.0x) | - | 45.70 (1.0x) |
| EPS1 | float32 | 13230000 | **2.49** | 5.01 (2.0x) | 4.38 (1.8x) | 4.08 (1.6x) |
| EPS1 | float64 | 13230000 | **2.49** | 6.27 (2.5x) | - | 4.83 (1.9x) |
| FS1 | float32 | 1323000 | **10.93** | 27.20 (2.5x) | 22.89 (2.1x) | 21.14 (1.9x) |
| FS1 | float64 | 1323000 | **10.93** | 51.72 (4.7x) | - | 45.97 (4.2x) |
| FS2 | float32 | 1323000 | **6.84** | 19.93 (2.9x) | 18.95 (2.8x) | 15.07 (2.2x) |
| FS2 | float64 | 1323000 | **6.84** | 45.28 (6.6x) | - | 43.45 (6.4x) |
| TS1 | float32 | 132300 | **7.90** | 18.34 (2.3x) | 16.36 (2.1x) | 13.62 (1.7x) |
| TS1 | float64 | 132300 | **7.90** | 32.75 (4.1x) | - | 42.49 (5.4x) |
| TS2 | float32 | 132300 | **7.30** | 17.97 (2.5x) | 15.96 (2.2x) | 13.57 (1.9x) |
| TS2 | float64 | 132300 | **7.30** | 32.54 (4.5x) | - | 42.50 (5.8x) |
| TS3 | float32 | 132300 | **5.29** | 16.54 (3.1x) | 14.77 (2.8x) | 12.98 (2.5x) |
| TS3 | float64 | 132300 | **5.29** | 28.01 (5.3x) | - | 41.61 (7.9x) |
| TS4 | float32 | 132300 | **5.50** | 16.09 (2.9x) | 14.36 (2.6x) | 12.69 (2.3x) |
| TS4 | float64 | 132300 | **5.50** | 26.09 (4.7x) | - | 41.28 (7.5x) |
| VS1 | float32 | 132300 | **6.78** | 24.05 (3.5x) | 22.32 (3.3x) | 21.83 (3.2x) |
| VS1 | float64 | 132300 | **6.78** | 45.95 (6.8x) | - | 46.42 (6.8x) |
| CE | float32 | 132300 | **10.29** | 22.55 (2.2x) | 20.41 (2.0x) | 19.74 (1.9x) |
| CE | float64 | 132300 | **10.29** | 51.26 (5.0x) | - | 46.76 (4.5x) |
| CP | float32 | 132300 | **6.29** | 22.06 (3.5x) | 20.25 (3.2x) | 19.78 (3.1x) |
| CP | float64 | 132300 | **6.29** | 43.99 (7.0x) | - | 44.86 (7.1x) |
| SE | float32 | 132300 | **8.74** | 12.88 (1.5x) | 12.12 (1.4x) | 16.45 (1.9x) |
| SE | float64 | 132300 | **8.75** | 26.72 (3.1x) | - | 41.11 (4.7x) |

What it shows:

- On the values as the rig wrote them (decimals), tsdb takes 2.5 to 11 bits per value, against 32
  or 64 uncompressed: it recognises the decimals and stores them as small whole numbers. Gorilla
  and Chimp take 1.4 to 3.5 times more space in float32 and 2.5 to 7 times more in float64.
- float32 and float64 are almost exactly the same size in tsdb on this data: a decimal costs the
  same whatever type it is stored in.
- Converted to psi, the values are no longer short decimals. In float32 tsdb still stays
  ahead (1.0 to 1.6 times smaller than the others); in float64 every codec, tsdb's included, needs
  16 to 45 bits per value.
