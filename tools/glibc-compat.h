/*
 * Force-included (-include) by `make dist`: keeps the binaries loadable by
 * an older glibc than the one they are built with.
 *
 * glibc 2.38 headers redirect strtol, sscanf and friends to __isoc23_* (C23
 * semantics: they also accept a 0b prefix) whenever _GNU_SOURCE is defined,
 * and those symbols do not exist before 2.38: EL9 has 2.34. tsdb never parses
 * 0b numbers, so references to them are bound to the old versions instead
 * (the scanf family to the C99 ones, __isoc99_*, not the pre-C99 GNU ones).
 * tools/check-glibc.sh verifies that no newer symbol remains.
 */
#if defined(__x86_64__) && defined(__linux__)
__asm__(".symver __isoc23_strtol,strtol@GLIBC_2.2.5");
__asm__(".symver __isoc23_strtoll,strtoll@GLIBC_2.2.5");
__asm__(".symver __isoc23_strtoul,strtoul@GLIBC_2.2.5");
__asm__(".symver __isoc23_strtoull,strtoull@GLIBC_2.2.5");
__asm__(".symver __isoc23_strtoimax,strtoimax@GLIBC_2.2.5");
__asm__(".symver __isoc23_strtoumax,strtoumax@GLIBC_2.2.5");
__asm__(".symver __isoc23_sscanf,__isoc99_sscanf@GLIBC_2.7");
__asm__(".symver __isoc23_vsscanf,__isoc99_vsscanf@GLIBC_2.7");
__asm__(".symver __isoc23_fscanf,__isoc99_fscanf@GLIBC_2.7");
__asm__(".symver __isoc23_scanf,__isoc99_scanf@GLIBC_2.7");
#endif
