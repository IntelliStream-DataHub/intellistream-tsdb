/* Self-test for `make asan`: signed overflow that UBSan must turn into a failing exit status.
 * Built and run by `make ubsan-check` with the sanitizer flags and options of `make asan`. */
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    int x = INT_MAX - 1 + (argc > 0 ? atoi(argc > 1 ? argv[1] : "0") : 0);
    x += 2 + argc; /* overflows: INT_MAX - 1 + 3 */
    printf("%d\n", x);
    return 0;
}
