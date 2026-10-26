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

Measured on the codecs branch, 2026-10-26, as background merges leave the parts (tsdb's codecs do not depend on the CPU, so you should get the same
numbers on any x86-64 machine).

| series | type | values | tsdb, bits/value | Gorilla + ZSTD | Chimp + ZSTD | byte split + ZSTD |
|---|---|---:|---:|---:|---:|---:|
| PS1 | float32 | 13230000 | **6.69** | 19.61 (2.9x) | 16.31 (2.4x) | 13.28 (2.0x) |
| PS1 | float64 | 13230000 | **6.70** | 31.10 (4.6x) | - | 35.20 (5.3x) |
| PS1 psi | float32 | 13230000 | **7.00** | 20.00 (2.9x) | 16.58 (2.4x) | 15.73 (2.2x) |
| PS1 psi | float64 | 13230000 | **7.19** | 43.73 (6.1x) | - | 46.96 (6.5x) |
| PS2 | float32 | 13230000 | **7.39** | 19.32 (2.6x) | 15.69 (2.1x) | 12.55 (1.7x) |
| PS2 | float64 | 13230000 | **7.60** | 29.30 (3.9x) | - | 31.67 (4.2x) |
| PS2 psi | float32 | 13230000 | **7.04** | 20.07 (2.9x) | 16.16 (2.3x) | 14.75 (2.1x) |
| PS2 psi | float64 | 13230000 | **7.40** | 43.48 (5.9x) | - | 41.45 (5.6x) |
| PS3 | float32 | 13230000 | **5.41** | 20.04 (3.7x) | 18.55 (3.4x) | 15.68 (2.9x) |
| PS3 | float64 | 13230000 | **5.43** | 22.79 (4.2x) | - | 35.99 (6.6x) |
| PS3 psi | float32 | 13230000 | **5.45** | 21.22 (3.9x) | 19.32 (3.5x) | 17.28 (3.2x) |
| PS3 psi | float64 | 13230000 | **5.59** | 30.23 (5.4x) | - | 39.59 (7.1x) |
| PS4 | float32 | 13230000 | **2.49** | 7.29 (2.9x) | 6.78 (2.7x) | 5.86 (2.4x) |
| PS4 | float64 | 13230000 | **2.50** | 15.82 (6.3x) | - | 15.09 (6.0x) |
| PS4 psi | float32 | 13230000 | **2.71** | 7.15 (2.6x) | 6.74 (2.5x) | 5.85 (2.2x) |
| PS4 psi | float64 | 13230000 | **2.85** | 16.43 (5.8x) | - | 15.85 (5.6x) |
| PS5 | float32 | 13230000 | **6.12** | 20.07 (3.3x) | 19.11 (3.1x) | 14.98 (2.4x) |
| PS5 | float64 | 13230000 | **6.13** | 43.60 (7.1x) | - | 44.02 (7.2x) |
| PS5 psi | float32 | 13230000 | **6.17** | 20.25 (3.3x) | 19.65 (3.2x) | 14.85 (2.4x) |
| PS5 psi | float64 | 13230000 | **6.18** | 44.63 (7.2x) | - | 45.75 (7.4x) |
| PS6 | float32 | 13230000 | **6.14** | 20.09 (3.3x) | 19.11 (3.1x) | 15.12 (2.5x) |
| PS6 | float64 | 13230000 | **6.15** | 44.09 (7.2x) | - | 44.03 (7.2x) |
| PS6 psi | float32 | 13230000 | **6.19** | 19.88 (3.2x) | 19.59 (3.2x) | 14.90 (2.4x) |
| PS6 psi | float64 | 13230000 | **6.20** | 44.93 (7.2x) | - | 45.70 (7.4x) |
| EPS1 | float32 | 13230000 | **2.49** | 5.01 (2.0x) | 4.38 (1.8x) | 4.08 (1.6x) |
| EPS1 | float64 | 13230000 | **2.49** | 6.27 (2.5x) | - | 4.83 (1.9x) |
| FS1 | float32 | 1323000 | **10.89** | 27.20 (2.5x) | 22.89 (2.1x) | 21.14 (1.9x) |
| FS1 | float64 | 1323000 | **10.93** | 51.72 (4.7x) | - | 45.97 (4.2x) |
| FS2 | float32 | 1323000 | **6.83** | 19.93 (2.9x) | 18.95 (2.8x) | 15.07 (2.2x) |
| FS2 | float64 | 1323000 | **6.84** | 45.28 (6.6x) | - | 43.45 (6.4x) |
| TS1 | float32 | 132300 | **5.92** | 18.34 (3.1x) | 16.36 (2.8x) | 13.62 (2.3x) |
| TS1 | float64 | 132300 | **6.60** | 32.75 (5.0x) | - | 42.49 (6.4x) |
| TS2 | float32 | 132300 | **5.66** | 17.97 (3.2x) | 15.96 (2.8x) | 13.57 (2.4x) |
| TS2 | float64 | 132300 | **6.07** | 32.54 (5.4x) | - | 42.50 (7.0x) |
| TS3 | float32 | 132300 | **5.02** | 16.54 (3.3x) | 14.77 (2.9x) | 12.98 (2.6x) |
| TS3 | float64 | 132300 | **5.05** | 28.01 (5.5x) | - | 41.61 (8.2x) |
| TS4 | float32 | 132300 | **5.04** | 16.09 (3.2x) | 14.36 (2.8x) | 12.69 (2.5x) |
| TS4 | float64 | 132300 | **5.31** | 26.09 (4.9x) | - | 41.28 (7.8x) |
| VS1 | float32 | 132300 | **6.78** | 24.05 (3.5x) | 22.32 (3.3x) | 21.83 (3.2x) |
| VS1 | float64 | 132300 | **6.78** | 45.95 (6.8x) | - | 46.42 (6.8x) |
| CE | float32 | 132300 | **10.29** | 22.55 (2.2x) | 20.41 (2.0x) | 19.74 (1.9x) |
| CE | float64 | 132300 | **10.29** | 51.26 (5.0x) | - | 46.76 (4.5x) |
| CP | float32 | 132300 | **6.29** | 22.06 (3.5x) | 20.25 (3.2x) | 19.78 (3.1x) |
| CP | float64 | 132300 | **6.29** | 43.99 (7.0x) | - | 44.86 (7.1x) |
| SE | float32 | 132300 | **8.75** | 12.88 (1.5x) | 12.12 (1.4x) | 16.45 (1.9x) |
| SE | float64 | 132300 | **8.75** | 26.72 (3.1x) | - | 41.11 (4.7x) |

What it shows:

- On the values as the rig wrote them (decimals), tsdb takes 2.5 to 11 bits per value, against 32
  or 64 uncompressed: it recognises the decimals and stores them as small whole numbers. Gorilla
  and Chimp take 1.4 to 3.7 times more space in float32 and 2.5 to 7.2 times more in float64.
- float32 and float64 are nearly the same size in tsdb on this data (within 0.7 bits per value): a
  decimal costs about the same whatever type it is stored in.
- Converted to psi, the values are no longer short decimals, but they still take only the few
  hundred values the pressure sensor's steps map to. tsdb's background merges store such a series
  as a dictionary of those values and each value's position in it: 2.7 to 7.4 bits per value in
  float32 and float64 alike, 2.1 to 3.9 times smaller than the others in float32 and 5.4 to 7.4
  times in float64. Freshly inserted data is stored at insert speed without the dictionary
  (about twice the bits in float32, up to eight times in float64) until a merge rewrites it.