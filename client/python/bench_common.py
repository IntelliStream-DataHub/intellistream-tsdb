"""What the Python benchmarks share with tools/cbench.h (the C benchmarks): the random query windows, the three
window classes, process CPU time and the result lines that bench/clients/run.sh reads. The constants and the
generator are copied from the C header; change them together."""

import time

T0 = 1_704_067_200_000  # 2024-01-01
# Step in ms of series s: tools/datasets.h ds_info[s % 7].step_ms.
STEPS = (60_000, 1_000, 10_000, 10_000, 1, 1_000, 1_000)
MASK = (1 << 64) - 1

# (id, label, agg, window samples, bucket samples)
CLASSES = (
    ("raw", "raw, 3600 samples", False, 3600, 0),
    ("agg300", "agg, 86400 samples in buckets of 300", True, 86_400, 300),
    ("agg3600", "agg, 604800 samples in buckets of 3600", True, 604_800, 3600),
)


def mix(x):
    x ^= x >> 33
    x = (x * 0xFF51AFD7ED558CCD) & MASK
    x ^= x >> 33
    x = (x * 0xC4CEB9FE1A85EC53) & MASK
    return x ^ (x >> 33)


def seed(thread):
    """The generator state of query thread `thread`."""
    return mix(thread + 991)


def step_ms(series):
    return STEPS[series % 7]


def window(r, nseries, points, window_samples):
    """The next random window of a thread: (new state, series, start, end)."""
    r = mix(r)
    s = r % nseries
    span = points - window_samples if points > window_samples else 1
    start = T0 + ((r >> 20) % span) * STEPS[s % 7]
    return r, s, start, start + window_samples * STEPS[s % 7]


def cpu_s():
    """CPU seconds (user + system, all threads) this process has used."""
    return time.process_time()


def report_query(client, klass, threads, queries, rows, secs, cpu):
    cid, label, _, _, _ = klass
    print(
        f"  {label:<44} {threads:>3} conn {queries / secs:>10.0f} q/s  {secs * threads / queries * 1e6:>8.1f} us/q"
        f"  {rows / secs / 1e6:>7.1f} Mrows/s"
    )
    print(
        f"RESULT kind=query client={client} class={cid} threads={threads} queries={queries} rows={rows}"
        f" secs={secs:.4f} cpu={cpu:.4f}",
        flush=True,
    )


def report_load(client, conns, compression, rows, secs, sent, cpu, extra=""):
    print(
        f"load ({client}): {rows} rows in {secs:.2f} s = {rows / secs / 1e6:.2f} M rows/s, {sent / rows:.2f} bytes/row,"
        f" client CPU {cpu:.2f} s ({cpu / (rows / 1e6):.2f} s per M rows), {conns} connections/threads{extra}"
    )
    print(
        f"RESULT kind=load client={client} conns={conns} compression={compression} rows={rows} secs={secs:.4f}"
        f" bytes={sent} cpu={cpu:.4f}",
        flush=True,
    )
