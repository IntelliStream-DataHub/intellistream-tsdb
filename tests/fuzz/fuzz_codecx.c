/*
 * fuzz_codec with an output buffer of exactly n values of the decoder's width (see fuzz_codec.c). Its corpus is
 * the codec's (the Makefile's FUZZ_CORPUS_codecx): the same bytes, a stricter buffer.
 */
#define CODEC_EXACT_OUT 1
#include "fuzz_codec.c"
