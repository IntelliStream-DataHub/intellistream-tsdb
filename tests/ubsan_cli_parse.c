/* The CLI's time and interval parsers (tools/common.h) on input past every
 * range, under UBSan with float-cast-overflow, which GCC's -fsanitize=undefined
 * leaves out: a double too large for int64 must be refused before the cast, not
 * cast and then refused (x86 turns such a cast into INT64_MIN, which hides the
 * undefined behaviour from a plain run). `make asan` runs it through ubsan-check;
 * the exit status is 0 only if every bad input is refused and every good one parses. */
#include "../tools/common.h"

static int failures;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #c);                                      \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)

int main(void)
{
    int64_t t;
    static const char *bad_times[] = {"2147483647-01-01", "now-1e18d", "now+9.223372e18", "now+9223372036854775000",
                                      "2024-01-01 99:99:99", "2024-02-31", "2023-02-29", "2023-13-01", "2023-01-00",
                                      "2024-01-01 00:60:00", "2024-01-01 00:00:60", "99999999999999999999",
                                      "-99999999999999999999", "2024-01-01 24:00:00", "2024-1-1x", "10-01-01",
                                      "2024-01-001", "2024--01-01", "now-1e300d", "now+inf", "now-nan"};
    for (size_t i = 0; i < sizeof bad_times / sizeof *bad_times; i++)
        if (parse_time(bad_times[i], &t)) {
            fprintf(stderr, "time accepted: %s\n", bad_times[i]);
            failures++;
        }
    static const char *bad_intervals[] = {"1e30", "inf", "-inf", "nan", "-1", "1e16d", "9223372036854775808",
                                          "1e300w", "9.3e18", "1e19ms"};
    for (size_t i = 0; i < sizeof bad_intervals / sizeof *bad_intervals; i++)
        if (parse_interval(bad_intervals[i], &t)) {
            fprintf(stderr, "interval accepted: %s\n", bad_intervals[i]);
            failures++;
        }
    CHECK(parse_time("2024-02-29", &t) && t == 1709164800000LL);
    CHECK(parse_time("2024-01-31 23:59:59.123", &t) && t == 1706745599123LL);
    CHECK(parse_time("2024-01-31T12:00:00Z", &t) && t == 1706702400000LL);
    CHECK(parse_time("1970-01-01", &t) && t == 0);
    CHECK(parse_time("1969-12-31", &t) && t == -86400000);
    CHECK(parse_time("-0001-01-01", &t));
    CHECK(parse_time("-1000", &t) && t == -1000);
    CHECK(parse_time("now-1h", &t));
    CHECK(parse_time("min", &t) && t == INT64_MIN);
    CHECK(parse_interval("1.5s", &t) && t == 1500);
    CHECK(parse_interval("1w", &t) && t == 604800000);
    CHECK(parse_interval("9223372036854775000", &t) && t > 0);
    if (failures) {
        fprintf(stderr, "ubsan_cli_parse: %d failures\n", failures);
        return 1;
    }
    puts("ubsan_cli_parse: ok");
    return 0;
}
