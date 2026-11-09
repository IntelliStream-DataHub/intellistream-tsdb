/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * USERS: who may log in to tsdbd and what each user may do (docs/design-auth.md §2, §4).
 *
 * The file is append-only records, each
 *
 *   [u32 len][u8 type][payload: len bytes][zeros to a multiple of 8][u64 check]
 *
 * where the check is xxh64 of everything before it, seeded with the record's file offset (as SERIES and CLASSES),
 * so that a record copied to another place, zeros or a torn append never pass. A change of more than one record
 * (USER and its VERIFIER) sets the type's high bit (REC_MORE) on all but its last, and counts only once that last
 * one is there. Records are 8-byte aligned, which
 * lets a damaged one be told from a torn tail: a valid record after a bad one is damage inside what was synced
 * (TSDB_ERR_CORRUPT); a bad record with nothing valid after it is an append that never finished, cut by the next
 * writer under the lock. Records:
 *
 *   HEADER   u32 format (1), u8 mock_key[32], u8 file_id[16]   first, written with the first change and by a
 *                                                              compaction (file_id random each time, mock_key kept)
 *   USER     u32 uid, u8 flags, u16 len, name          uid above every earlier one
 *   VERIFIER u32 uid, u32 iterations, u8 salt_len, salt, u8 stored_key[32], u8 server_key[32]
 *   FLAGS    u32 uid, u8 flags
 *   GRANT    u32 uid, u8 kind, u8 rights, u16 len, selector   the resulting rights on that selector (0 removes it)
 *   DROP     u32 uid
 *
 * A record that checks out but makes no sense (a GRANT for a user that does not exist, a uid that goes back, a
 * selector not in normal form) is damage too: skipping it would change who may read what without a word (A11).
 *
 * Every change runs under an OFD write lock on the file, so the CLI appends safely while tsdbd runs (and over NFS,
 * where it is a server lock): lock, read what others appended (cutting a torn tail), check the change against that
 * state, append, fdatasync (and fsync the directory when the file is new), apply, unlock. A failed write or sync is
 * fail-stop: an acknowledged revoke must not come back after a power loss, and a retried fsync proves nothing.
 * The failed records are cut off again before the lock goes: after a failed writeback the kernel marks their pages
 * clean, and a retry that read them back would find its change made, write nothing and acknowledge a change that
 * may never reach the disk. For the same reason an answer that changes nothing is given only after an fdatasync
 * (the records it rests on may be the unsynced append of a writer that died).
 * A compaction writes the live state to a new file and renames it over USERS; whoever holds the old file sees
 * another inode at the path and reads the new one from the start. The inode number alone does not tell: a reader
 * that closes the file between reloads (tsdbd's tick, a follower) can find a later compaction's file under the
 * number of the one it read (ext4 reuses them, NFS fileids too), with the same size when a demotion or a new
 * password compacted to as many bytes. So every reload also compares the HEADER's file_id and the last record it
 * read, which a new file does not have at the same place. The compactor locks the new file before the rename and
 * keeps it locked until the directory fsync: a writer that opens the new name meanwhile would otherwise append to
 * a file whose name a power loss can still take back.
 *
 * The file is created, and compacted, with the owner of the database directory (an operator running the CLI as
 * root must not leave a USERS the service user cannot read) and mode 0600; an existing one readable by others is
 * set to 0600, or refused when that fails: it holds the verifiers, which are offline guessing targets, and with one
 * recorded login of a user a way to log in as that user (StoredKey turns its ClientProof into ClientKey).
 * Both files are opened through the directory's descriptor with O_NOFOLLOW (USERS.compact new, with O_EXCL, after
 * unlinking whatever had the name), and only a regular file with one link is chowned, chmodded or read: whoever can
 * write the directory must not get a writer running as root to create, overwrite or chmod a file elsewhere.
 *
 * Loading applies records in a batch with copy-on-write of the users it touches, so that a reload that meets damage
 * halfway leaves the state exactly as it was (A11: the last good state stays).
 */
#define _GNU_SOURCE
#include "users.h"

#include "tsdb.h"
#include "../client/tsdb_crypto.h"
#include "../src/compat.h" /* tsdb_fsync, tsdb_fdatasync (macOS: with the drive's cache flush); nothing new on Linux */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

uint64_t xxh64(const void *data, size_t len, uint64_t seed); /* the engine's (src/internal.h) */

enum { REC_HEADER = 1, REC_USER, REC_VERIFIER, REC_FLAGS, REC_GRANT, REC_DROP };
#define REC_MORE 0x80 /* in the type: the change goes on in the next record (USER then VERIFIER) */
#define USERS_FORMAT 1
#define REC_PAYLOAD_MAX (4 + 1 + 1 + 2 + SEL_MAX)
#define HEADER_LEN (4 + 32 + 16)
#define HEADER_SIZE 72 /* rec_size(HEADER_LEN) */

static size_t rec_size(size_t len) { return ((5 + len + 7) & ~(size_t)7) + 8; }

static uint16_t ld16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }
static uint32_t ld32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint64_t ld64(const uint8_t *p) { return ld32(p) | (uint64_t)ld32(p + 4) << 32; }

/* ------------------------------------------------------------ selectors */

bool user_name_valid(const char *name, size_t len)
{
    if (len < 1 || len > USER_NAME_MAX || name[0] == '/')
        return false;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)name[i];
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || strchr("._-@/:+", c)) ||
            c == 0)
            return false;
    }
    return true;
}

bool verifier_valid(const user_verifier *v)
{
    return v->salt_len >= SALT_MIN && v->salt_len <= SALT_MAX && v->iterations >= ITER_MIN;
}

static bool special(char c) { return c == '*' || c == '?' || c == '\\'; }

int sel_normalize(uint8_t kind, const char *in, size_t len, uint8_t *out_kind, char *out, size_t *out_len)
{
    if (memchr(in, 0, len))
        return TSDB_ERR_INVALID; /* series names are C strings */
    switch (kind) {
    case SEL_ALL:
        if (len)
            return TSDB_ERR_INVALID;
        break;
    case SEL_NAME:
        if (len < 1 || len > SEL_MAX)
            return TSDB_ERR_INVALID;
        break;
    case SEL_PREFIX:
        if (len > SEL_MAX)
            return TSDB_ERR_INVALID;
        if (!len)
            kind = SEL_ALL; /* the empty prefix is every name: one form per set */
        break;
    case SEL_GLOB: {
        if (len < 1 || len > GLOB_MAX)
            return TSDB_ERR_INVALID;
        /* Tokens: literal bytes, '*', '?'. A run of '*' is one; only the special bytes stay escaped. lit gets the
         * unescaped literal bytes, which are the whole set when there is no wildcard. */
        char lit[GLOB_MAX];
        size_t n = 0, nl = 0, stars = 0, qs = 0;
        bool prev_star = false;
        for (size_t i = 0; i < len; i++) {
            char c = in[i];
            if (c == '*') {
                if (!prev_star)
                    out[n++] = '*', stars++;
                prev_star = true;
                continue;
            }
            prev_star = false;
            if (c == '?') {
                out[n++] = '?', qs++;
                continue;
            }
            if (c == '\\') {
                if (i + 1 == len)
                    return TSDB_ERR_INVALID; /* a lone '\' at the end escapes nothing */
                c = in[++i];
            }
            if (special(c))
                out[n++] = '\\';
            out[n++] = c;
            lit[nl++] = c;
        }
        if (stars + qs == 0) {
            memcpy(out, lit, nl);
            n = nl, kind = SEL_NAME;
        } else if (qs == 0 && stars == 1 && prev_star) {
            memcpy(out, lit, nl);
            n = nl, kind = nl ? SEL_PREFIX : SEL_ALL;
        }
        out[n] = '\0';
        *out_kind = kind;
        *out_len = n;
        return 0;
    }
    default:
        return TSDB_ERR_INVALID;
    }
    memcpy(out, in, len);
    out[len] = '\0';
    *out_kind = kind;
    *out_len = len;
    return 0;
}

int sel_parse(const char *text, uint8_t *kind, const char **sel, size_t *len)
{
    static const struct {
        const char *tag;
        uint8_t kind;
    } k[] = {{"prefix:", SEL_PREFIX}, {"name:", SEL_NAME}, {"glob:", SEL_GLOB}};
    if (strcmp(text, "all") == 0) {
        *kind = SEL_ALL, *sel = "", *len = 0;
        return 0;
    }
    for (size_t i = 0; i < 3; i++) {
        size_t t = strlen(k[i].tag);
        if (strncmp(text, k[i].tag, t) == 0) {
            *kind = k[i].kind, *sel = text + t, *len = strlen(text + t);
            return 0;
        }
    }
    return TSDB_ERR_INVALID;
}

static const struct {
    const char *w;
    uint8_t r;
} right_words[] = {{"read", R_READ},   {"insert", R_INSERT}, {"delete", R_DELETE},
                   {"admin", R_ADMIN}, {"write", R_WRITE},   {"none", 0}};

static int hex_digit(char c)
{
    return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1;
}

int rights_parse(const char *text, uint8_t *out)
{
    uint8_t r = 0;
    for (const char *p = text;;) {
        size_t n = strcspn(p, ","), next = n;
        while (n && *p == ' ')
            p++, n--, next--;
        while (n && p[n - 1] == ' ')
            n--;
        size_t i;
        for (i = 0; i < sizeof right_words / sizeof right_words[0]; i++)
            if (strlen(right_words[i].w) == n && strncmp(p, right_words[i].w, n) == 0)
                break;
        if (i < sizeof right_words / sizeof right_words[0])
            r |= right_words[i].r;
        else if (n == 4 && p[0] == '0' && p[1] == 'x' && hex_digit(p[2]) >= 0 && hex_digit(p[3]) >= 0)
            r |= (uint8_t)(hex_digit(p[2]) << 4 | hex_digit(p[3]));
        else
            return TSDB_ERR_INVALID;
        if (!p[next])
            return *out = r, 0;
        p += next + 1;
    }
}

const char *rights_text(uint8_t r, char buf[RIGHTS_TEXT_MAX])
{
    size_t n = 0;
    buf[0] = '\0';
    for (size_t i = 0; i < 4; i++)
        if (r & right_words[i].r)
            n += (size_t)snprintf(buf + n, RIGHTS_TEXT_MAX - n, "%s%s", n ? "," : "", right_words[i].w);
    if (r & ~R_EVERY)
        snprintf(buf + n, RIGHTS_TEXT_MAX - n, "%s0x%02x", n ? "," : "", r & ~R_EVERY);
    else if (!r)
        snprintf(buf, RIGHTS_TEXT_MAX, "none");
    return buf;
}

/* A glob compiled for a bit-parallel run (A24). Its tokens are a byte, '?' or '*' (a run of '*' is one); bit j of
 * the state says the name read so far can end where token j starts, bit ntok that it matches all of the glob. A
 * byte moves every live bit over a token that takes it (that byte or '?') one place on and keeps every live bit at
 * a '*'; a live '*' also makes the token after it live (it may match nothing). Each byte of the name thus costs
 * the same few word operations whatever the glob's shape: linear in the name, where the greedy matcher this
 * replaces backtracked to the last '*' (64 globs of 256 bytes against one name of 32767 bytes took 0.3 s). */
#define GLOB_WORDS 5 /* GLOB_MAX tokens and the end */

struct glob_prog {
    uint16_t ntok;
    uint8_t words;     /* ceil((ntok + 1) / 64), at most GLOB_WORDS */
    bool last_star;    /* the last token is a '*': once it is live, every rest of the name matches */
    uint16_t cls[256]; /* byte -> its row of m; 0: a byte no token names, which only '?' takes */
    uint64_t star[GLOB_WORDS];
    uint64_t m[];      /* rows of `words` words: the tokens that take the byte */
};

enum { T_BYTE, T_ANY, T_STAR };

/* The tokens of g; SIZE_MAX when there are more than GLOB_WORDS words hold. */
static size_t glob_tokens(const char *g, size_t gl, uint8_t *kind, uint8_t *byte)
{
    size_t n = 0;
    for (size_t p = 0; p < gl; p++) {
        uint8_t k = g[p] == '*' ? T_STAR : g[p] == '?' ? T_ANY : T_BYTE, c = (uint8_t)g[p];
        if (k == T_STAR && n && kind[n - 1] == T_STAR)
            continue;
        if (g[p] == '\\' && p + 1 < gl) /* a lone '\' at the end is the byte itself (sel_normalize refuses it) */
            c = (uint8_t)g[++p];
        if (n == 64 * GLOB_WORDS - 1)
            return SIZE_MAX;
        kind[n] = k, byte[n] = c;
        n++;
    }
    return n;
}

/* Compiles g into p (NULL: only sizes it). The bytes p needs, or 0 for a glob of too many tokens. */
static size_t glob_compile(const char *g, size_t gl, struct glob_prog *p)
{
    uint8_t kind[64 * GLOB_WORDS], byte[64 * GLOB_WORDS];
    uint16_t cls[256] = {0};
    size_t n = glob_tokens(g, gl, kind, byte);
    if (n == SIZE_MAX)
        return 0;
    size_t ncls = 1, words = (n + 1 + 63) / 64;
    for (size_t j = 0; j < n; j++)
        if (kind[j] == T_BYTE && !cls[byte[j]])
            cls[byte[j]] = (uint16_t)ncls++;
    size_t size = sizeof(struct glob_prog) + ncls * words * 8;
    if (!p)
        return size;
    memset(p, 0, size);
    p->ntok = (uint16_t)n, p->words = (uint8_t)words, p->last_star = n && kind[n - 1] == T_STAR;
    memcpy(p->cls, cls, sizeof cls);
    for (size_t j = 0; j < n; j++) {
        uint64_t bit = 1ull << (j & 63);
        if (kind[j] == T_STAR) {
            p->star[j >> 6] |= bit;
        } else if (kind[j] == T_ANY) {
            for (size_t c = 0; c < ncls; c++)
                p->m[c * words + (j >> 6)] |= bit;
        } else {
            p->m[cls[byte[j]] * words + (j >> 6)] |= bit;
        }
    }
    return size;
}

/* w words of state; inlined for each w, so the word loops unroll and the state stays in registers */
static inline __attribute__((always_inline)) bool glob_run_w(const struct glob_prog *p, const char *s, size_t sl,
                                                             const size_t w)
{
    size_t end = p->ntok, last = end - 1; /* last: only read when last_star, so ntok > 0 */
    uint64_t d[GLOB_WORDS] = {1}, st[GLOB_WORDS];
    for (size_t k = 0; k < w; k++)
        st[k] = p->star[k];
    d[0] |= (d[0] & st[0]) << 1;
    for (size_t i = 0; i < sl; i++) {
        if (p->last_star && (d[last >> 6] >> (last & 63) & 1))
            return true;
        const uint64_t *row = &p->m[p->cls[(uint8_t)s[i]] * w];
        uint64_t carry = 0, any = 0;
        for (size_t k = 0; k < w; k++) {
            uint64_t x = d[k] & row[k];
            d[k] = (x << 1) | carry | (d[k] & st[k]);
            carry = x >> 63;
        }
        carry = 0; /* a live '*' lets the token after it start too; never two '*' in a row, so one pass */
        for (size_t k = 0; k < w; k++) {
            uint64_t e = d[k] & st[k];
            d[k] |= (e << 1) | carry;
            carry = e >> 63;
            any |= d[k];
        }
        if (!any)
            return false;
    }
    return d[end >> 6] >> (end & 63) & 1;
}

static bool glob_run(const struct glob_prog *p, const char *s, size_t sl)
{
    switch (p->words) {
    case 1: return glob_run_w(p, s, sl, 1); /* up to 63 tokens: the common case */
    case 2: return glob_run_w(p, s, sl, 2);
    case 3: return glob_run_w(p, s, sl, 3);
    case 4: return glob_run_w(p, s, sl, 4);
    default: return glob_run_w(p, s, sl, GLOB_WORDS);
    }
}

/* No recursion, no allocation (the program lives on the stack: at most 257 rows); the name's length x
 * ceil((tokens + 1) / 64) word steps. A glob of more tokens than GLOB_WORDS words hold matches nothing: none is ever
 * stored (GLOB_MAX, A24). */
bool glob_match(const char *g, size_t gl, const char *s, size_t sl)
{
    uint64_t buf[(sizeof(struct glob_prog) + 257 * GLOB_WORDS * 8) / 8 + 1];
    struct glob_prog *p = (struct glob_prog *)buf;
    size_t need = glob_compile(g, gl, NULL);
    if (!need || need > sizeof buf)
        return false;
    glob_compile(g, gl, p);
    return glob_run(p, s, sl);
}

/* A stored glob runs the program its grant was given (glob_parts). */
static bool grant_glob(const user_grant *g, const char *s, size_t sl)
{
    return g->prog ? glob_run(g->prog, s, sl) : glob_match(g->sel, g->len, s, sl);
}

static bool starts_with(const char *s, size_t sl, const char *p, size_t pl) { return sl >= pl && memcmp(s, p, pl) == 0; }

bool grant_matches(const user_grant *g, const char *name, size_t len)
{
    switch (g->kind) {
    case SEL_ALL: return true;
    case SEL_PREFIX: return starts_with(name, len, g->sel, g->len);
    case SEL_NAME: return len == g->len && memcmp(name, g->sel, len) == 0;
    default: return grant_glob(g, name, len);
    }
}

bool sel_inside(const user_grant *s, const user_grant *t)
{
    switch (t->kind) {
    case SEL_ALL: return true;
    case SEL_PREFIX:
        switch (s->kind) {
        case SEL_PREFIX:
        case SEL_NAME: return starts_with(s->sel, s->len, t->sel, t->len);
        case SEL_GLOB: return starts_with(s->lit, s->lit_len, t->sel, t->len);
        default: return false;
        }
    case SEL_GLOB:
        if (s->kind == SEL_NAME)
            return grant_glob(t, s->sel, s->len);
        return s->kind == SEL_GLOB && s->len == t->len && memcmp(s->sel, t->sel, s->len) == 0;
    default: return s->kind == SEL_NAME && s->len == t->len && memcmp(s->sel, t->sel, s->len) == 0;
    }
}

static bool superuser(const user *x) { return !x || (x->flags & USER_SUPERUSER); }

/* s lies inside one of x's admin selectors. The slow forms try every grant of x; the others (after rights_of) use
 * x's index, so that managing a user of many grants by an admin of many grants is not their product. */
static bool inside_admin_slow(const user *x, const user_grant *s)
{
    for (uint32_t i = 0; i < x->ng; i++)
        if ((x->g[i].rights & R_ADMIN) && sel_inside(s, &x->g[i]))
            return true;
    return false;
}

static bool inside_admin(const user *x, const user_grant *s);
static bool names_under(const user *x, const char *name);

/* x holds admin on all, or on a prefix of the name: user names are made and managed under prefixes only. */
static bool names_under_slow(const user *x, const char *name)
{
    size_t len = strlen(name);
    for (uint32_t i = 0; i < x->ng; i++)
        if ((x->g[i].rights & R_ADMIN) &&
            (x->g[i].kind == SEL_ALL || (x->g[i].kind == SEL_PREFIX && starts_with(name, len, x->g[i].sel, x->g[i].len))))
            return true;
    return false;
}

bool user_manages(const user *x, const user *u)
{
    if (superuser(x))
        return true;
    if ((u->flags & USER_SUPERUSER) || !names_under(x, u->name))
        return false;
    for (uint32_t i = 0; i < u->ng; i++)
        if (!inside_admin(x, &u->g[i]))
            return false; /* a right x could not have given: x may not take u over */
    return true;
}

bool may_create(const user *x, const char *name, uint8_t flags)
{
    if (superuser(x))
        return true;
    return !(flags & USER_SUPERUSER) && names_under(x, name);
}

bool may_grant(const user *x, const user *u, const user_grant *s)
{
    return user_manages(x, u) && (superuser(x) || inside_admin(x, s));
}

bool may_revoke(const user *x, const user_grant *s) { return superuser(x) || inside_admin(x, s); }

bool may_passwd(const user *x, const user *u) { return !x || x->uid == u->uid || user_manages(x, u); }

bool may_list(const user *x, const user *u) { return !x || x->uid == u->uid || user_manages(x, u); }

/* ------------------------------------------------------- rights_of index */

/* One open-addressing table holds the name grants, the prefix grants (keyed by their bytes) and the globs (keyed by
 * their literal prefix, chained through gnext); a name is probed once as a name, once per distinct prefix length and
 * once per distinct literal-prefix length (§2 "Matching"). */
typedef struct ment {
    const char *key; /* NULL: empty slot */
    uint16_t len;
    uint8_t kind, rights;
    int32_t glob; /* SEL_GLOB: the first grant of the chain */
} ment;

struct umatch {
    uint8_t all;
    ment *t;
    size_t mask;
    uint16_t *plen, *glen; /* distinct lengths, ascending */
    size_t np, ngl;
    int32_t *gnext;
};

static size_t mhash(uint8_t kind, const char *k, size_t len) { return (size_t)xxh64(k, len, 0x9e3779b97f4a7c15ull * (kind + 1)); }

static ment *mfind(const struct umatch *m, uint8_t kind, const char *k, size_t len)
{
    for (size_t h = mhash(kind, k, len) & m->mask;; h = (h + 1) & m->mask) {
        ment *e = &m->t[h];
        if (!e->key || (e->kind == kind && e->len == len && memcmp(e->key, k, len) == 0))
            return e;
    }
}

static void umatch_free(struct umatch *m)
{
    if (m) {
        free(m->t), free(m->plen), free(m->glen), free(m->gnext);
        free(m);
    }
}

static void add_len(uint16_t *a, size_t *n, uint16_t len)
{
    size_t i = 0;
    while (i < *n && a[i] < len)
        i++;
    if (i < *n && a[i] == len)
        return;
    memmove(a + i + 1, a + i, (*n - i) * sizeof *a);
    a[i] = len;
    (*n)++;
}

static struct umatch *umatch_build(const user *u)
{
    struct umatch *m = calloc(1, sizeof *m);
    size_t cap = 16;
    while (cap < 2 * (size_t)u->ng + 2)
        cap *= 2;
    if (!m || !(m->t = calloc(cap, sizeof *m->t)) || !(m->plen = malloc((u->ng + 1) * sizeof *m->plen)) ||
        !(m->glen = malloc((u->ng + 1) * sizeof *m->glen)) || !(m->gnext = malloc((u->ng + 1) * sizeof *m->gnext))) {
        umatch_free(m);
        return NULL;
    }
    m->mask = cap - 1;
    for (uint32_t i = 0; i < u->ng; i++) {
        const user_grant *g = &u->g[i];
        if (g->kind == SEL_ALL) {
            m->all |= g->rights;
            continue;
        }
        const char *k = g->kind == SEL_GLOB ? g->lit : g->sel;
        uint16_t len = g->kind == SEL_GLOB ? g->lit_len : g->len;
        ment *e = mfind(m, g->kind, k, len);
        if (!e->key) {
            *e = (ment){.key = k, .len = len, .kind = g->kind, .glob = -1};
            if (g->kind == SEL_PREFIX)
                add_len(m->plen, &m->np, len);
            else if (g->kind == SEL_GLOB)
                add_len(m->glen, &m->ngl, len);
        }
        if (g->kind == SEL_GLOB) {
            m->gnext[i] = e->glob;
            e->glob = (int32_t)i;
        } else {
            e->rights |= g->rights;
        }
    }
    return m;
}

static uint8_t expand(uint8_t r) { return (r & R_ADMIN) ? R_EVERY : r; }

uint8_t rights_of_slow(const user *u, const char *name, size_t len)
{
    if (u->flags & USER_SUPERUSER)
        return R_EVERY;
    uint8_t r = 0;
    for (uint32_t i = 0; i < u->ng; i++)
        if (grant_matches(&u->g[i], name, len))
            r |= u->g[i].rights;
    return expand(r);
}

uint8_t rights_of(user *u, const char *name, size_t len)
{
    if (u->flags & USER_SUPERUSER)
        return R_EVERY;
    if (!u->m && !(u->m = umatch_build(u)))
        return rights_of_slow(u, name, len); /* out of memory: the same answer, slower */
    const struct umatch *m = u->m;
    uint8_t r = m->all;
    const ment *e;
    if (len <= SEL_MAX && (e = mfind(m, SEL_NAME, name, len))->key)
        r |= e->rights;
    for (size_t i = 0; i < m->np && m->plen[i] <= len; i++)
        if ((e = mfind(m, SEL_PREFIX, name, m->plen[i]))->key)
            r |= e->rights;
    for (size_t i = 0; i < m->ngl && m->glen[i] <= len; i++) {
        if (!(e = mfind(m, SEL_GLOB, name, m->glen[i]))->key)
            continue;
        for (int32_t gi = e->glob; gi >= 0; gi = m->gnext[gi]) {
            const user_grant *g = &u->g[gi];
            if ((r | g->rights) != r && len >= (size_t)g->lit_len + g->suf_len &&
                memcmp(name + len - g->suf_len, g->suf, g->suf_len) == 0 && grant_glob(g, name, len))
                r |= g->rights;
        }
    }
    return expand(r);
}

bool user_index_build(user *u) { return u->m || (u->m = umatch_build(u)); }

/* x's index, built on first use; a cache, so it is built through a const user too (users are never const objects). */
static const struct umatch *umatch_of(const user *x)
{
    user *w = (user *)x;
    if (!w->m)
        w->m = umatch_build(w);
    return w->m;
}

/* A glob admin grant of x matches the name. */
static bool admin_glob_matches(const user *x, const struct umatch *m, const char *name, size_t len)
{
    const ment *e;
    for (size_t i = 0; i < m->ngl && m->glen[i] <= len; i++) {
        if (!(e = mfind(m, SEL_GLOB, name, m->glen[i]))->key)
            continue;
        for (int32_t gi = e->glob; gi >= 0; gi = m->gnext[gi]) {
            const user_grant *g = &x->g[gi];
            if ((g->rights & R_ADMIN) && len >= (size_t)g->lit_len + g->suf_len &&
                memcmp(name + len - g->suf_len, g->suf, g->suf_len) == 0 && grant_glob(g, name, len))
                return true;
        }
    }
    return false;
}

/* An admin prefix grant of x is a prefix of key. */
static bool admin_prefix_of(const struct umatch *m, const char *key, size_t len)
{
    const ment *e;
    for (size_t i = 0; i < m->np && m->plen[i] <= len; i++)
        if ((e = mfind(m, SEL_PREFIX, key, m->plen[i]))->key && (e->rights & R_ADMIN))
            return true;
    return false;
}

/* sel_inside(s, t) for some admin grant t of x, by the table of §2 read the other way: t is all; a prefix of s's
 * bytes (a glob's literal prefix); for a name, the same name or a glob that matches it; for a glob, the same glob. */
static bool inside_admin(const user *x, const user_grant *s)
{
    const struct umatch *m = umatch_of(x);
    if (!m)
        return inside_admin_slow(x, s);
    if (m->all & R_ADMIN)
        return true;
    if (s->kind == SEL_ALL)
        return false;
    const char *k = s->kind == SEL_GLOB ? s->lit : s->sel;
    size_t kl = s->kind == SEL_GLOB ? s->lit_len : s->len;
    if (admin_prefix_of(m, k, kl))
        return true;
    const ment *e;
    if (s->kind == SEL_NAME)
        return ((e = mfind(m, SEL_NAME, k, kl))->key && (e->rights & R_ADMIN)) || admin_glob_matches(x, m, k, kl);
    if (s->kind == SEL_GLOB && (e = mfind(m, SEL_GLOB, k, kl))->key)
        for (int32_t gi = e->glob; gi >= 0; gi = m->gnext[gi])
            if ((x->g[gi].rights & R_ADMIN) && x->g[gi].len == s->len && memcmp(x->g[gi].sel, s->sel, s->len) == 0)
                return true;
    return false;
}

static bool names_under(const user *x, const char *name)
{
    const struct umatch *m = umatch_of(x);
    if (!m)
        return names_under_slow(x, name);
    return (m->all & R_ADMIN) || admin_prefix_of(m, name, strlen(name));
}

/* ------------------------------------------------------- users in memory */

static void grant_clear(user_grant *g)
{
    free(g->sel), free(g->lit), free(g->suf), free(g->prog);
    memset(g, 0, sizeof *g);
}

static void user_free(user *u)
{
    if (!u)
        return;
    for (uint32_t i = 0; i < u->ng; i++)
        grant_clear(&u->g[i]);
    free(u->g);
    umatch_free(u->m);
    tsdb_wipe(&u->v, sizeof u->v);
    free(u);
}

/* The unescaped literal prefix and suffix of a normalized glob, and its program. */
static int glob_parts(user_grant *g)
{
    size_t ps = glob_compile(g->sel, g->len, NULL);
    g->lit = malloc(g->len + 1);
    g->suf = malloc(g->len + 1);
    g->prog = ps ? malloc(ps) : NULL; /* none for a glob of too many tokens: it matches nothing either way */
    if (!g->lit || !g->suf || (ps && !g->prog))
        return TSDB_ERR_NOMEM;
    if (ps)
        glob_compile(g->sel, g->len, g->prog);
    size_t nl = 0, ns = 0;
    bool wild = false;
    for (size_t i = 0; i < g->len; i++) {
        char c = g->sel[i];
        if (c == '*' || c == '?') {
            wild = true;
            ns = 0;
            continue;
        }
        if (c == '\\')
            c = g->sel[++i];
        if (!wild)
            g->lit[nl++] = c;
        g->suf[ns++] = c;
    }
    g->lit_len = (uint16_t)nl, g->suf_len = (uint16_t)ns;
    return 0;
}

static int grant_init(user_grant *g, uint8_t kind, uint8_t rights, const char *sel, size_t len)
{
    memset(g, 0, sizeof *g);
    g->kind = kind, g->rights = rights, g->len = (uint16_t)len;
    if (!(g->sel = malloc(len + 1)))
        return TSDB_ERR_NOMEM;
    memcpy(g->sel, sel, len);
    g->sel[len] = '\0';
    int rc = kind == SEL_GLOB ? glob_parts(g) : 0;
    if (rc)
        grant_clear(g);
    return rc;
}

static user *user_dup(const user *u)
{
    user *c = malloc(sizeof *c);
    if (!c)
        return NULL;
    *c = *u;
    c->m = NULL;
    c->g = NULL;
    c->ng = c->gcap = 0;
    if (u->ng && !(c->g = calloc(u->ng, sizeof *c->g))) {
        free(c);
        return NULL;
    }
    c->gcap = u->ng;
    for (uint32_t i = 0; i < u->ng; i++, c->ng++)
        if (grant_init(&c->g[i], u->g[i].kind, u->g[i].rights, u->g[i].sel, u->g[i].len) != 0) {
            user_free(c);
            return NULL;
        }
    return c;
}

user *user_copy(const user *u) { return user_dup(u); }
void user_release(user *u) { user_free(u); }

static long user_index(const users *us, uint32_t uid)
{
    size_t lo = 0, hi = us->n;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (us->u[mid]->uid < uid)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < us->n && us->u[lo]->uid == uid ? (long)lo : -1;
}

const user *users_by_uid(const users *us, uint32_t uid)
{
    long i = user_index(us, uid);
    return i < 0 ? NULL : us->u[i];
}

/* The name table: linear probing on the name's hash, holding uids (users move in the array, their uids do not; 0 is
 * an empty slot, uids start at 1), at most half full. It changes with every USER and DROP applied, and is rebuilt
 * after a rollback; without memory for it, users_find scans. */
struct uname {
    uint32_t uid, h;
};

static uint32_t name_hash(const char *name) { return (uint32_t)xxh64(name, strlen(name), 0x6e616d6573ull); }

static void names_put(users *us, const user *u)
{
    uint32_t h = name_hash(u->name), i = h & us->names_mask;
    while (us->names[i].uid)
        i = (i + 1) & us->names_mask;
    us->names[i] = (struct uname){u->uid, h};
    us->names_n++;
}

static int names_rebuild(users *us)
{
    free(us->names);
    us->names = NULL;
    us->names_n = us->names_mask = 0;
    uint32_t cap = 16;
    while (cap < 2 * (uint64_t)us->n + 2)
        cap *= 2;
    if (!(us->names = calloc(cap, sizeof *us->names)))
        return TSDB_ERR_NOMEM;
    us->names_mask = cap - 1;
    for (uint32_t i = 0; i < us->n; i++)
        names_put(us, us->u[i]);
    return 0;
}

/* u was just added to us->u */
static int names_add(users *us, const user *u)
{
    if (!us->names || 2 * ((uint64_t)us->names_n + 1) > (uint64_t)us->names_mask + 1)
        return names_rebuild(us);
    names_put(us, u);
    return 0;
}

static void names_del(users *us, const user *u)
{
    if (!us->names)
        return;
    struct uname *t = us->names;
    uint32_t m = us->names_mask, j = name_hash(u->name) & m;
    while (t[j].uid && t[j].uid != u->uid)
        j = (j + 1) & m;
    if (!t[j].uid)
        return;
    /* backward shift: an entry after the hole moves into it unless its home slot lies in (hole, entry] */
    for (uint32_t k = (j + 1) & m; t[k].uid; k = (k + 1) & m) {
        uint32_t home = t[k].h & m;
        if (j < k ? (home <= j || home > k) : (home <= j && home > k)) {
            t[j] = t[k];
            j = k;
        }
    }
    t[j].uid = 0;
    us->names_n--;
}

const user *users_find(const users *us, const char *name)
{
    if (!us->names) {
        for (uint32_t i = 0; i < us->n; i++)
            if (strcmp(us->u[i]->name, name) == 0)
                return us->u[i];
        return NULL;
    }
    uint32_t h = name_hash(name);
    for (uint32_t i = h & us->names_mask; us->names[i].uid; i = (i + 1) & us->names_mask)
        if (us->names[i].h == h) {
            const user *u = users_by_uid(us, us->names[i].uid);
            if (u && strcmp(u->name, name) == 0)
                return u;
        }
    return NULL;
}

uint32_t users_superusers(const users *us)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < us->n; i++)
        n += (us->u[i]->flags & USER_SUPERUSER) != 0;
    return n;
}

static long grant_find(const user *u, uint8_t kind, const char *sel, size_t len)
{
    for (uint32_t i = 0; i < u->ng; i++)
        if (u->g[i].kind == kind && u->g[i].len == len && memcmp(u->g[i].sel, sel, len) == 0)
            return i;
    return -1;
}

static uint32_t glob_count(const user *u)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < u->ng; i++)
        n += u->g[i].kind == SEL_GLOB;
    return n;
}

/* ------------------------------------------------------- load batches */

/* A batch applies records to copies of the users it touches; the originals stay as they were until the commit, so
 * a rollback restores the state exactly. */
typedef struct ptrs {
    user **p;
    size_t n, cap;
} ptrs;

typedef struct batch {
    user **saved; /* the array of live users before */
    uint32_t saved_n, saved_max;
    bool saved_hdr;
    uint8_t saved_key[32], saved_id[16];
    uint64_t saved_off, saved_last_off, saved_last_check;
    ptrs old;  /* originals copied or dropped: freed by a commit */
    ptrs dead; /* copies or new users dropped within the batch: freed either way */
} batch;

static int ptrs_push(ptrs *a, user *u)
{
    if (a->n == a->cap) {
        size_t cap = a->cap ? 2 * a->cap : 16;
        user **p = realloc(a->p, cap * sizeof *p);
        if (!p)
            return TSDB_ERR_NOMEM;
        a->p = p, a->cap = cap;
    }
    a->p[a->n++] = u;
    return 0;
}

static int batch_begin(users *us, batch *b)
{
    memset(b, 0, sizeof *b);
    us->batch++;
    if (us->n && !(b->saved = malloc(us->n * sizeof *b->saved)))
        return TSDB_ERR_NOMEM;
    if (us->n)
        memcpy(b->saved, us->u, us->n * sizeof *b->saved);
    b->saved_n = us->n, b->saved_max = us->max_uid, b->saved_hdr = us->have_header;
    memcpy(b->saved_key, us->mock_key, 32);
    memcpy(b->saved_id, us->file_id, 16);
    b->saved_off = us->off, b->saved_last_off = us->last_off, b->saved_last_check = us->last_check;
    return 0;
}

static void batch_commit(users *us, batch *b)
{
    for (size_t i = 0; i < b->old.n; i++)
        user_free(b->old.p[i]);
    for (size_t i = 0; i < b->dead.n; i++)
        user_free(b->dead.p[i]);
    free(b->old.p), free(b->dead.p), free(b->saved);
}

static void batch_rollback(users *us, batch *b)
{
    for (uint32_t i = 0; i < us->n; i++)
        if (us->u[i]->batch == us->batch)
            user_free(us->u[i]);
    for (size_t i = 0; i < b->dead.n; i++)
        user_free(b->dead.p[i]);
    if (b->saved_n)
        memcpy(us->u, b->saved, b->saved_n * sizeof *us->u); /* the array only grew or shrank in place */
    us->n = b->saved_n, us->max_uid = b->saved_max, us->have_header = b->saved_hdr;
    memcpy(us->mock_key, b->saved_key, 32);
    memcpy(us->file_id, b->saved_id, 16);
    us->off = b->saved_off, us->last_off = b->saved_last_off, us->last_check = b->saved_last_check;
    free(b->old.p), free(b->dead.p), free(b->saved);
    (void)names_rebuild(us); /* out of memory: users_find scans until the next rebuild */
}

/* The user at index i, made the batch's own copy. */
static user *touch(users *us, batch *b, long i)
{
    user *u = us->u[i];
    if (u->batch == us->batch)
        return u;
    user *c = user_dup(u);
    if (!c || ptrs_push(&b->old, u) != 0) {
        user_free(c);
        return NULL;
    }
    c->batch = us->batch;
    us->u[i] = c;
    return c;
}

static int drop_at(users *us, batch *b, long i)
{
    user *u = us->u[i];
    int rc = ptrs_push(u->batch == us->batch ? &b->dead : &b->old, u);
    if (rc == 0) {
        names_del(us, u);
        memmove(us->u + i, us->u + i + 1, (us->n - (size_t)i - 1) * sizeof *us->u);
        us->n--;
    }
    return rc;
}

/* Every user goes: the start of a reread from offset 0. */
static int drop_all(users *us, batch *b)
{
    while (us->n)
        if (drop_at(us, b, (long)us->n - 1) != 0)
            return TSDB_ERR_NOMEM;
    us->max_uid = 0, us->have_header = false;
    us->off = us->last_off = us->last_check = 0;
    return 0;
}

/* Applies one record; TSDB_ERR_CORRUPT for one that does not fit the state. */
static int apply_rec(users *us, batch *b, uint8_t type, const uint8_t *p, size_t len, uint64_t off)
{
    if ((off == 0) != (type == REC_HEADER))
        return TSDB_ERR_CORRUPT; /* the header is the first record and only the first */
    if (type == REC_HEADER) {
        if (len != HEADER_LEN || ld32(p) != USERS_FORMAT)
            return TSDB_ERR_CORRUPT;
        memcpy(us->mock_key, p + 4, 32);
        memcpy(us->file_id, p + 36, 16);
        us->have_header = true;
        return 0;
    }
    if (len < 4 || type < REC_USER || type > REC_DROP)
        return TSDB_ERR_CORRUPT;
    uint32_t uid = ld32(p);
    if (type == REC_USER) {
        if (len < 7 || len != 7u + ld16(p + 5) || uid <= us->max_uid || (p[4] & ~USER_SUPERUSER))
            return TSDB_ERR_CORRUPT;
        size_t nl = ld16(p + 5);
        char name[USER_NAME_MAX + 1];
        if (!user_name_valid((const char *)p + 7, nl))
            return TSDB_ERR_CORRUPT;
        memcpy(name, p + 7, nl);
        name[nl] = '\0';
        if (users_find(us, name))
            return TSDB_ERR_CORRUPT;
        user *u = calloc(1, sizeof *u);
        if (!u)
            return TSDB_ERR_NOMEM;
        u->uid = uid, u->flags = p[4], u->batch = us->batch;
        memcpy(u->name, name, nl + 1);
        if (us->n == us->cap) {
            uint32_t cap = us->cap ? 2 * us->cap : 16;
            user **a = realloc(us->u, cap * sizeof *a);
            if (!a) {
                free(u);
                return TSDB_ERR_NOMEM;
            }
            us->u = a, us->cap = cap;
        }
        us->u[us->n++] = u; /* uids only grow: the array stays sorted */
        if (names_add(us, u) != 0) {
            us->n--;
            free(u);
            return TSDB_ERR_NOMEM;
        }
        us->max_uid = uid;
        return 0;
    }
    long i = user_index(us, uid);
    if (i < 0)
        return TSDB_ERR_CORRUPT;
    if (type == REC_DROP)
        return len == 4 ? drop_at(us, b, i) : TSDB_ERR_CORRUPT;
    if (type == REC_FLAGS && (len != 5 || (p[4] & ~USER_SUPERUSER)))
        return TSDB_ERR_CORRUPT;
    user_verifier v;
    if (type == REC_VERIFIER) {
        memset(&v, 0, sizeof v);
        if (len < 9 || len != 9u + p[8] + 64)
            return TSDB_ERR_CORRUPT;
        v.iterations = ld32(p + 4), v.salt_len = p[8];
        if (!verifier_valid(&v))
            return TSDB_ERR_CORRUPT;
        memcpy(v.salt, p + 9, v.salt_len);
        memcpy(v.stored_key, p + 9 + v.salt_len, 32);
        memcpy(v.server_key, p + 9 + v.salt_len + 32, 32);
    }
    uint8_t kind = 0, rights = 0;
    const char *sel = NULL;
    size_t sl = 0;
    if (type == REC_GRANT) {
        if (len < 8 || len != 8u + ld16(p + 6))
            return TSDB_ERR_CORRUPT;
        kind = p[4], rights = p[5], sel = (const char *)p + 8, sl = ld16(p + 6);
        /* stored normalized (a glob within its cap); anything else was not written by a writer */
        char norm[GLOB_MAX + 1];
        uint8_t nk = kind;
        size_t nlen = sl;
        if (rights > R_EVERY || kind > SEL_GLOB || memchr(sel, 0, sl) || (kind == SEL_ALL) != (sl == 0) ||
            (kind == SEL_GLOB && (sl > GLOB_MAX || sel_normalize(kind, sel, sl, &nk, norm, &nlen) != 0 ||
                                  nk != kind || nlen != sl || memcmp(norm, sel, sl) != 0)))
            return TSDB_ERR_CORRUPT;
    }
    user *u = touch(us, b, i);
    if (!u)
        return TSDB_ERR_NOMEM;
    if (type == REC_FLAGS) {
        u->flags = p[4];
        return 0;
    }
    if (type == REC_VERIFIER) {
        u->v = v;
        u->has_verifier = true;
        tsdb_wipe(&v, sizeof v);
        return 0;
    }
    umatch_free(u->m);
    u->m = NULL;
    long gi = grant_find(u, kind, sel, sl);
    if (gi >= 0 && rights) {
        u->g[gi].rights = rights;
    } else if (gi >= 0) {
        grant_clear(&u->g[gi]);
        memmove(u->g + gi, u->g + gi + 1, (u->ng - (size_t)gi - 1) * sizeof *u->g);
        u->ng--;
    } else if (rights) {
        if (kind == SEL_GLOB && glob_count(u) >= USER_GLOBS)
            return TSDB_ERR_CORRUPT;
        if (u->ng == u->gcap) {
            uint32_t cap = u->gcap ? 2 * u->gcap : 4;
            user_grant *g = realloc(u->g, cap * sizeof *g);
            if (!g)
                return TSDB_ERR_NOMEM;
            u->g = g, u->gcap = cap;
        }
        int rc = grant_init(&u->g[u->ng], kind, rights, sel, sl);
        if (rc)
            return rc;
        u->ng++;
    }
    return 0;
}

/* The record at buf[pos..size), at file offset base + pos. */
static bool parse_rec(const uint8_t *buf, size_t pos, size_t size, uint64_t base, uint8_t *type, const uint8_t **pl,
                      size_t *len, size_t *rs)
{
    if (pos + 16 > size)
        return false;
    const uint8_t *r = buf + pos;
    *len = ld32(r);
    if (*len > REC_PAYLOAD_MAX || rec_size(*len) > size - pos)
        return false;
    *rs = rec_size(*len);
    for (size_t i = 5 + *len; i < *rs - 8; i++)
        if (r[i])
            return false;
    if (xxh64(r, *rs - 8, base + pos) != ld64(r + *rs - 8))
        return false;
    *type = r[4];
    *pl = r + 5;
    return true;
}

static bool valid_after(const uint8_t *buf, size_t from, size_t size, uint64_t base)
{
    uint8_t type;
    const uint8_t *pl;
    size_t len, rs;
    for (size_t q = from + 8; q < size; q += 8) /* from is 8-aligned in the file, base too */
        if (parse_rec(buf, q, size, base, &type, &pl, &len, &rs))
            return true;
    return false;
}

/* Applies the whole changes of buf (at file offset base) from pos; *end: where the first one that is not whole
 * starts (a change whose last record is missing is not whole: its records go with it, so that a user is never
 * left created without the verifier it was created with). TSDB_ERR_CORRUPT when a valid record follows a bad one,
 * or a record does not fit. */
static int apply_buf(users *us, batch *b, const uint8_t *buf, size_t size, uint64_t base, size_t pos, size_t *end)
{
    uint8_t type;
    const uint8_t *pl;
    size_t len, rs, p = pos, whole = pos;
    while (parse_rec(buf, p, size, base, &type, &pl, &len, &rs)) {
        p += rs;
        if (!(type & REC_MORE))
            whole = p;
    }
    *end = pos;
    if (p != size && valid_after(buf, p, size, base)) {
        fprintf(stderr, "tsdb: %s is damaged at offset %llu, before valid records\n", us->path,
                (unsigned long long)(base + p));
        return TSDB_ERR_CORRUPT;
    }
    int rc = 0;
    while (rc == 0 && pos < whole && parse_rec(buf, pos, size, base, &type, &pl, &len, &rs)) {
        rc = apply_rec(us, b, type & ~REC_MORE, pl, len, base + pos);
        if (rc == TSDB_ERR_CORRUPT)
            fprintf(stderr, "tsdb: %s: the record at offset %llu (type %u) does not fit what came before it\n",
                    us->path, (unsigned long long)(base + pos), type);
        if (rc == 0) {
            us->last_off = base + pos;
            us->last_check = ld64(buf + pos + rs - 8);
            pos += rs;
        }
    }
    *end = pos;
    return rc;
}

static int read_at(int fd, uint8_t *buf, size_t len, uint64_t off)
{
    for (size_t got = 0; got < len;) {
        ssize_t r = pread(fd, buf + got, len - got, (off_t)(off + got));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return TSDB_ERR_IO; /* a short read too: never taken for the end */
        got += (size_t)r;
    }
    return 0;
}

/* Reads fd from where the state stands (or from the start, when it is another file or was cut) and applies what is
 * whole. Under the write lock (cut), an incomplete tail is an append that died: it is cut once a second read from
 * the disk agrees (a misread must not cut acknowledged records, as in SERIES). */
static int catch_up(users *us, int fd, bool cut)
{
    struct stat st;
    if (fstat(fd, &st) != 0)
        return TSDB_ERR_IO;
    uint64_t size = (uint64_t)st.st_size;
    bool full = st.st_ino != us->ino || st.st_dev != us->dev || size < us->off || !us->off;
    uint8_t *buf = NULL;
    uint64_t from = 0;
    int rc = 0;
    if (!full) {
        /* another file under the same inode number (a reader that closed the old one lets ext4 or an NFS server hand
         * it to a later compaction) has another file_id, even at the same size */
        uint8_t hdr[HEADER_SIZE], type;
        const uint8_t *pl;
        size_t len, rs;
        if (size < HEADER_SIZE)
            full = true;
        else if ((rc = read_at(fd, hdr, HEADER_SIZE, 0)) != 0)
            return rc;
        else if (!parse_rec(hdr, 0, HEADER_SIZE, 0, &type, &pl, &len, &rs) || type != REC_HEADER ||
                 len != HEADER_LEN || memcmp(pl + 36, us->file_id, 16) != 0)
            full = true;
    }
    if (!full) {
        /* the last record read is read again (also when nothing was appended): a file cut and appended to since is
         * read from the start */
        from = us->last_off;
        size_t n = (size_t)(size - from);
        uint8_t type;
        const uint8_t *pl;
        size_t len, rs;
        if (!(buf = malloc(n)))
            return TSDB_ERR_NOMEM;
        if ((rc = read_at(fd, buf, n, from)) != 0) {
            free(buf);
            return rc;
        }
        if (!parse_rec(buf, 0, n, from, &type, &pl, &len, &rs) || from + rs != us->off ||
            ld64(buf + rs - 8) != us->last_check) {
            free(buf), buf = NULL;
            full = true;
        } else if (size == us->off) {
            free(buf);
            return 0; /* the same file, nothing new */
        }
    }
    if (full) {
        from = 0;
        if (!(buf = malloc(size ? size : 1)))
            return TSDB_ERR_NOMEM;
        if ((rc = read_at(fd, buf, (size_t)size, 0)) != 0) {
            free(buf);
            return rc;
        }
    }
    batch b;
    if ((rc = batch_begin(us, &b)) != 0) {
        free(buf);
        return rc;
    }
    if (full)
        rc = drop_all(us, &b);
    size_t end = 0;
    if (rc == 0)
        rc = apply_buf(us, &b, buf, (size_t)(size - from), from, full ? 0 : (size_t)(us->off - from), &end);
    if (rc != 0) {
        batch_rollback(us, &b);
        free(buf);
        return rc;
    }
    us->off = from + end;
    us->ino = st.st_ino, us->dev = st.st_dev;
    batch_commit(us, &b);
    if (cut && us->off < size) {
        uint64_t n = size - us->off;
        uint8_t *again = malloc((size_t)n);
        (void)posix_fadvise(fd, (off_t)us->off, (off_t)n, POSIX_FADV_DONTNEED);
        rc = !again ? TSDB_ERR_NOMEM : read_at(fd, again, (size_t)n, us->off);
        if (rc == 0 && memcmp(again, buf + (us->off - from), (size_t)n) != 0) {
            fprintf(stderr, "tsdb: %s: two reads of its tail at offset %llu differ: nothing is cut\n", us->path,
                    (unsigned long long)us->off);
            rc = TSDB_ERR_IO;
        }
        free(again);
        if (rc == 0) {
            fprintf(stderr, "tsdb: %s: cutting a torn tail of %llu bytes at offset %llu\n", us->path,
                    (unsigned long long)n, (unsigned long long)us->off);
            if (ftruncate(fd, (off_t)us->off) != 0)
                rc = TSDB_ERR_IO;
        }
    }
    free(buf);
    return rc;
}

static int file_perms(const char *path, int fd, const struct stat *d);

/* The directory, opened once (again only while it is missing): the files in it are reached through this descriptor,
 * so a directory renamed or replaced meanwhile is not mixed with the one opened. */
static int users_dirfd(users *us)
{
    if (us->dfd < 0)
        us->dfd = open(us->dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    return us->dfd;
}

/* USERS or USERS.compact, never through a symbolic link: whoever can write the directory could otherwise point the
 * name at any file, and a writer running as root (an embedding program; the tsdb CLI drops to the directory's owner
 * first) would create, truncate, chown or chmod that file instead (secrel cli #1). */
static int open_in_dir(users *us, const char *name, int flags, mode_t mode)
{
    int dfd = users_dirfd(us);
    if (dfd < 0)
        return -1;
    int fd = openat(dfd, name, flags | O_NOFOLLOW | O_CLOEXEC, mode);
    if (fd < 0 && errno == ELOOP) {
        fprintf(stderr, "tsdb: %s/%s is a symbolic link: refused (tsdb makes it as a regular file)\n", us->dir, name);
        errno = ELOOP;
    }
    return fd;
}

int users_open(users *us, const char *dir, bool writable)
{
    memset(us, 0, sizeof *us);
    us->fd = -1;
    us->dfd = -1;
    us->writable = writable;
    if (strlen(dir) >= sizeof us->dir)
        return TSDB_ERR_INVALID;
    snprintf(us->dir, sizeof us->dir, "%s", dir);
    snprintf(us->path, sizeof us->path, "%s/USERS", dir);
    int fd = open_in_dir(us, "USERS", O_RDONLY, 0);
    if (fd < 0)
        return errno == ENOENT ? 0 : TSDB_ERR_IO;
    int rc = file_perms(us->path, fd, NULL); /* the mode; the owner is a writer's business */
    if (rc == 0)
        rc = catch_up(us, fd, false);
    close(fd);
    return rc;
}

int users_reload(users *us)
{
    /* opened again each time: on NFS that revalidates the size (close-to-open consistency) */
    int fd = open_in_dir(us, "USERS", O_RDONLY, 0);
    int rc;
    if (fd < 0 && errno == ENOENT && us->ino) {
        fprintf(stderr, "tsdb: %s is gone: the users stay as they were read\n", us->path);
        rc = TSDB_ERR_CORRUPT;
    } else if (fd < 0) {
        rc = errno == ENOENT ? 0 : TSDB_ERR_IO;
    } else {
        rc = catch_up(us, fd, false);
        close(fd);
    }
    if (rc == TSDB_ERR_CORRUPT)
        us->damaged = true;
    return rc;
}

void users_close(users *us)
{
    for (uint32_t i = 0; i < us->n; i++)
        user_free(us->u[i]);
    free(us->u);
    free(us->names);
    if (us->fd >= 0)
        close(us->fd);
    if (us->dfd >= 0)
        close(us->dfd);
    tsdb_wipe(us->mock_key, sizeof us->mock_key);
    memset(us, 0, sizeof *us);
    us->fd = -1;
    us->dfd = -1;
}

/* ------------------------------------------------------- appending */

typedef struct rbuf {
    uint8_t *p;
    size_t n, cap;
    uint64_t base; /* file offset of p[0] */
    size_t rec;    /* start of the record being built */
    bool oom;
    bool more; /* the records built now are followed by more of the same change */
} rbuf;

static void put(rbuf *b, const void *d, size_t n)
{
    if (b->oom)
        return;
    if (b->n + n > b->cap) {
        size_t cap = b->cap ? b->cap : 256;
        while (cap < b->n + n)
            cap *= 2;
        uint8_t *p = realloc(b->p, cap);
        if (!p) {
            b->oom = true;
            return;
        }
        b->p = p, b->cap = cap;
    }
    memcpy(b->p + b->n, d, n);
    b->n += n;
}

static void put8(rbuf *b, uint8_t v) { put(b, &v, 1); }
static void put16(rbuf *b, uint16_t v) { put(b, (uint8_t[]){(uint8_t)v, (uint8_t)(v >> 8)}, 2); }
static void put32(rbuf *b, uint32_t v)
{
    put(b, (uint8_t[]){(uint8_t)v, (uint8_t)(v >> 8), (uint8_t)(v >> 16), (uint8_t)(v >> 24)}, 4);
}

static void rec_begin(rbuf *b, uint8_t type)
{
    b->rec = b->n;
    put32(b, 0);
    put8(b, b->more ? type | REC_MORE : type);
}

static void rec_end(rbuf *b)
{
    if (b->oom)
        return;
    size_t len = b->n - b->rec - 5;
    uint8_t *r = b->p + b->rec;
    r[0] = (uint8_t)len, r[1] = (uint8_t)(len >> 8), r[2] = (uint8_t)(len >> 16), r[3] = (uint8_t)(len >> 24);
    static const uint8_t zeros[8];
    put(b, zeros, rec_size(len) - 8 - (5 + len));
    if (b->oom)
        return;
    uint64_t h = xxh64(b->p + b->rec, b->n - b->rec, b->base + b->rec);
    put32(b, (uint32_t)h);
    put32(b, (uint32_t)(h >> 32));
}

/* A HEADER for a new file: its own random file_id */
static void rec_header(rbuf *b, const uint8_t key[32])
{
    uint8_t id[16];
    if (tsdb_random(id, sizeof id) != 0) {
        b->oom = true;
        return;
    }
    rec_begin(b, REC_HEADER);
    put32(b, USERS_FORMAT);
    put(b, key, 32);
    put(b, id, 16);
    rec_end(b);
}

static void rec_user(rbuf *b, uint32_t uid, uint8_t flags, const char *name)
{
    rec_begin(b, REC_USER);
    put32(b, uid);
    put8(b, flags);
    put16(b, (uint16_t)strlen(name));
    put(b, name, strlen(name));
    rec_end(b);
}

static void rec_verifier(rbuf *b, uint32_t uid, const user_verifier *v)
{
    rec_begin(b, REC_VERIFIER);
    put32(b, uid);
    put32(b, v->iterations);
    put8(b, v->salt_len);
    put(b, v->salt, v->salt_len);
    put(b, v->stored_key, 32);
    put(b, v->server_key, 32);
    rec_end(b);
}

static void rec_u8(rbuf *b, uint8_t type, uint32_t uid, int flags)
{
    rec_begin(b, type);
    put32(b, uid);
    if (flags >= 0)
        put8(b, (uint8_t)flags);
    rec_end(b);
}

static void rec_grant(rbuf *b, uint32_t uid, uint8_t kind, uint8_t rights, const char *sel, size_t len)
{
    rec_begin(b, REC_GRANT);
    put32(b, uid);
    put8(b, kind);
    put8(b, rights);
    put16(b, (uint16_t)len);
    put(b, sel, len);
    rec_end(b);
}

static void users_fail(users *us, const char *what)
{
    char msg[4400];
    snprintf(msg, sizeof msg, "%s of %s failed: %s; nothing more is acknowledged", what, us->path, strerror(errno));
    fprintf(stderr, "tsdb: %s\n", msg);
    us->failed = true;
    if (us->on_fail)
        us->on_fail(us->fail_arg, msg);
}

static int lock_fd(int fd, short type)
{
    struct flock fl = {.l_type = type, .l_whence = SEEK_SET};
    int r;
    while ((r = fcntl(fd, F_OFD_SETLKW, &fl)) != 0 && errno == EINTR)
        ;
    return r == 0 ? 0 : TSDB_ERR_IO;
}

static void unlock(users *us)
{
    if (us->fd >= 0)
        (void)lock_fd(us->fd, F_UNLCK);
}

static int fsync_dir(const char *dir)
{
    int fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    int r = tsdb_fsync(fd);
    int e = errno;
    close(fd);
    errno = e;
    return r;
}

/* A writer runs as the owner of the database directory, or as root, who gives the files it makes to that owner:
 * `sudo tsdb <dir> user add` must not leave a USERS that tsdbd, running as the directory's owner, cannot open. */
static int writer_ok(users *us, struct stat *d)
{
    int dfd = users_dirfd(us);
    if (dfd < 0 || fstat(dfd, d) != 0)
        return TSDB_ERR_IO;
    uid_t me = geteuid();
    if (me != 0 && me != d->st_uid) {
        fprintf(stderr,
                "tsdb: %s belongs to uid %u and this runs as uid %u: change users as the directory's owner (or "
                "root), so that USERS stays readable by the server\n",
                us->dir, (unsigned)d->st_uid, (unsigned)me);
        return TSDB_ERR_IO;
    }
    return 0;
}

/* USERS (or a compaction's new file) gets the directory's owner and group and mode 0600: it holds the verifiers,
 * which are offline guessing targets. A file that keeps another owner, or group or other bits, is refused. The group
 * is best effort (the owner may not be in the directory's group; with mode 0600 it grants nothing).
 * Only a regular file with no other name is changed or read: a hard link (or a device or FIFO) put in the directory
 * would have the owner and mode of another file changed, and tsdb never makes one there. */
static int file_perms(const char *path, int fd, const struct stat *d)
{
    struct stat st;
    if (fstat(fd, &st) != 0)
        return TSDB_ERR_IO;
    if (!S_ISREG(st.st_mode) || st.st_nlink != 1) {
        fprintf(stderr, "tsdb: %s is %s: refused (tsdb makes it as a regular file with no other name)\n", path,
                !S_ISREG(st.st_mode) ? "not a regular file" : "a file with more than one name (a hard link)");
        return TSDB_ERR_IO;
    }
    if (d && (st.st_uid != d->st_uid || st.st_gid != d->st_gid) && fchown(fd, d->st_uid, d->st_gid) != 0 &&
        st.st_uid != d->st_uid) {
        fprintf(stderr, "tsdb: %s belongs to uid %u, not to the directory's owner (uid %u), and cannot be given to it: "
                        "%s\n", path, (unsigned)st.st_uid, (unsigned)d->st_uid, strerror(errno));
        return TSDB_ERR_IO;
    }
    if (st.st_mode & 077) {
        if (fchmod(fd, 0600) != 0) {
            fprintf(stderr, "tsdb: %s has mode %03o (readable by others) and cannot be set to 0600: %s\n", path,
                    (unsigned)(st.st_mode & 0777), strerror(errno));
            return TSDB_ERR_IO;
        }
        fprintf(stderr, "tsdb: %s had mode %03o: set to 0600\n", path, (unsigned)(st.st_mode & 0777));
    }
    return 0;
}

/* Cuts the records of a change that failed off the file again: after a failed writeback their pages are clean but
 * may not be on the disk, and a retry that read them would take its change as made. Best effort: the process
 * is failing anyway. */
static void cut_failed(users *us, uint64_t base)
{
    int e = errno;
    if (ftruncate(us->fd, (off_t)base) != 0)
        fprintf(stderr, "tsdb: %s: the failed change could not be cut off again: %s\n", us->path, strerror(errno));
    errno = e;
}

/* The directory entry of the file being changed is durable: fsynced by this process since that file got its name.
 * A writer that dies between creating (or a compactor between renaming) and the directory fsync leaves a name a
 * power loss can still take back, with records that the next writer would otherwise build on. */
static int sync_name(users *us, const uint8_t id[16])
{
    if (us->name_synced && memcmp(us->synced_id, id, 16) == 0)
        return 0;
    if (fsync_dir(us->dir) != 0) {
        users_fail(us, "the directory fsync");
        return TSDB_ERR_IO;
    }
    memcpy(us->synced_id, id, 16);
    us->name_synced = true;
    return 0;
}

/* An answer that changes nothing (or says a user or a grant is not there), given under the lock: the state it rests
 * on may hold records of a writer that died between its write and its fdatasync, which a power loss can take back,
 * so they are made durable first. Unlocks. */
static int change_none(users *us, int rc)
{
    if (tsdb_fdatasync(us->fd) != 0) {
        users_fail(us, "fdatasync");
        rc = TSDB_ERR_IO;
    } else if (us->have_header && sync_name(us, us->file_id) != 0) {
        rc = TSDB_ERR_IO;
    }
    unlock(us);
    return rc;
}

/* Takes the write lock on the file now at the path, and reads what others appended. */
static int change_lock(users *us)
{
    if (!us->writable)
        return TSDB_ERR_READONLY;
    if (us->failed)
        return TSDB_ERR_IO;
    if (us->damaged)
        return TSDB_ERR_CORRUPT;
    struct stat d;
    int rc = writer_ok(us, &d);
    if (rc)
        return rc;
    for (;;) {
        if (us->fd < 0) {
            /* created only by the first change ever seen: a file that went away is not replaced by an empty one,
             * which would drop every user without a word */
            us->fd = open_in_dir(us, "USERS", O_RDWR | O_APPEND | (us->ino ? 0 : O_CREAT), 0600);
            if (us->fd < 0) {
                if (errno == ENOENT) {
                    fprintf(stderr, "tsdb: %s is gone: no change is made\n", us->path);
                    us->damaged = true;
                    return TSDB_ERR_CORRUPT;
                }
                return TSDB_ERR_IO;
            }
            if ((rc = file_perms(us->path, us->fd, &d)) != 0) {
                close(us->fd);
                us->fd = -1;
                return rc;
            }
        }
        if (lock_fd(us->fd, F_WRLCK) != 0)
            return TSDB_ERR_IO;
        struct stat a, p;
        if (fstat(us->fd, &a) != 0) {
            unlock(us);
            return TSDB_ERR_IO;
        }
        if (fstatat(us->dfd, "USERS", &p, AT_SYMLINK_NOFOLLOW) == 0 && p.st_ino == a.st_ino && p.st_dev == a.st_dev)
            break;
        /* a compaction put another file in its place while this one waited */
        unlock(us);
        close(us->fd);
        us->fd = -1;
    }
    rc = catch_up(us, us->fd, true);
    if (rc == TSDB_ERR_CORRUPT)
        us->damaged = true;
    if (rc)
        unlock(us);
    return rc;
}

/* Appends the records of b (built for offset us->off), syncs and applies them, and unlocks. */
static int change_write(users *us, rbuf *b)
{
    int rc = 0;
    if (b->oom) {
        rc = TSDB_ERR_NOMEM;
        goto out;
    }
    /* the file's id: in the header that goes with the change when the file is new */
    uint8_t id[16];
    memcpy(id, us->off ? us->file_id : b->p + 5 + 36, 16);
    for (size_t done = 0; done < b->n;) {
        ssize_t w = write(us->fd, b->p + done, b->n - done);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0) {
            if (w == 0)
                errno = EIO;
            cut_failed(us, b->base); /* before users_fail: tsdbd's on_fail exits */
            users_fail(us, "a write");
            rc = TSDB_ERR_IO;
            goto out;
        }
        done += (size_t)w;
    }
    if (tsdb_fdatasync(us->fd) != 0) {
        cut_failed(us, b->base);
        users_fail(us, "fdatasync");
        rc = TSDB_ERR_IO;
        goto out;
    }
    if ((rc = sync_name(us, id)) != 0)
        goto out;
    batch bt;
    size_t end;
    if ((rc = batch_begin(us, &bt)) == 0) {
        rc = apply_buf(us, &bt, b->p, b->n, b->base, 0, &end);
        if (rc == 0 && end != b->n)
            rc = TSDB_ERR_CORRUPT;
        if (rc == 0) {
            us->off = b->base + b->n;
            batch_commit(us, &bt);
        } else {
            batch_rollback(us, &bt);
        }
    }
    if (rc) {
        /* durable but not in memory: the next reader applies it; this process must not go on as if it were not */
        errno = rc == TSDB_ERR_NOMEM ? ENOMEM : EINVAL;
        users_fail(us, "applying a change");
        rc = TSDB_ERR_IO;
    }
out:
    unlock(us);
    free(b->p);
    return rc;
}

static void change_buf(users *us, rbuf *b)
{
    memset(b, 0, sizeof *b);
    b->base = us->off;
    if (us->off == 0) {
        uint8_t key[32];
        if (tsdb_random(key, sizeof key) != 0) {
            b->oom = true;
            return;
        }
        rec_header(b, key);
        tsdb_wipe(key, sizeof key);
    }
}

/* The actor after the catch-up: NULL for the operator; *x NULL with DENIED for a user dropped meanwhile. */
static int actor_of(users *us, uint32_t actor, const user **x)
{
    *x = actor ? users_by_uid(us, actor) : NULL;
    return actor && !*x ? TSDB_ERR_DENIED : 0;
}

/* A user that does not exist: whoever could create the name learns that; anyone else sees DENIED, as for a user
 * they may not touch (no listing of other tenants' users by trying names). */
static int missing(const user *x, const char *name) { return may_create(x, name, 0) ? TSDB_ERR_NOTFOUND : TSDB_ERR_DENIED; }

int users_create(users *us, uint32_t actor, const char *name, uint8_t flags, const user_verifier *v, uint32_t *uid)
{
    if (!user_name_valid(name, strlen(name)) || (flags & ~USER_SUPERUSER) || (v && !verifier_valid(v)))
        return TSDB_ERR_INVALID;
    int rc = change_lock(us);
    if (rc)
        return rc;
    const user *x;
    if ((rc = actor_of(us, actor, &x)) == 0 && !may_create(x, name, flags))
        rc = TSDB_ERR_DENIED;
    if (rc == 0 && (users_find(us, name) || us->max_uid == UINT32_MAX))
        rc = TSDB_ERR_INVALID;
    if (rc) {
        unlock(us);
        return rc;
    }
    uint32_t id = us->max_uid + 1;
    rbuf b;
    change_buf(us, &b);
    b.more = v != NULL; /* a user is never left without the password it was created with */
    rec_user(&b, id, flags, name);
    b.more = false;
    if (v)
        rec_verifier(&b, id, v);
    rc = change_write(us, &b);
    if (rc == 0 && uid)
        *uid = id;
    return rc;
}

/* Takes the lock and finds the target and the actor; on an error the lock is released. */
static int change_target(users *us, uint32_t actor, const char *name, const user **x, const user **u)
{
    if (!user_name_valid(name, strlen(name)))
        return TSDB_ERR_INVALID;
    int rc = change_lock(us);
    if (rc)
        return rc;
    if ((rc = actor_of(us, actor, x)) == 0 && !(*u = users_find(us, name)))
        rc = missing(*x, name);
    if (rc == TSDB_ERR_NOTFOUND)
        return change_none(us, rc);
    if (rc)
        unlock(us);
    return rc;
}

int users_drop(users *us, uint32_t actor, const char *name)
{
    const user *x, *u;
    int rc = change_target(us, actor, name, &x, &u);
    if (rc)
        return rc;
    /* the last superuser goes only offline: tsdbd then refuses to start until one is added (A13) */
    if (!user_manages(x, u) || (actor && (u->flags & USER_SUPERUSER) && users_superusers(us) == 1)) {
        unlock(us);
        return TSDB_ERR_DENIED;
    }
    rbuf b;
    change_buf(us, &b);
    rec_u8(&b, REC_DROP, u->uid, -1);
    return change_write(us, &b);
}

int users_set_verifier(users *us, uint32_t actor, const char *name, const user_verifier *v)
{
    if (!verifier_valid(v))
        return TSDB_ERR_INVALID;
    const user *x, *u;
    int rc = change_target(us, actor, name, &x, &u);
    if (rc)
        return rc;
    if (!may_passwd(x, u)) {
        unlock(us);
        return TSDB_ERR_DENIED;
    }
    rbuf b;
    change_buf(us, &b);
    rec_verifier(&b, u->uid, v);
    return change_write(us, &b);
}

int users_set_flags(users *us, uint32_t actor, const char *name, uint8_t flags)
{
    if (flags & ~USER_SUPERUSER)
        return TSDB_ERR_INVALID;
    const user *x, *u;
    int rc = change_target(us, actor, name, &x, &u);
    if (rc)
        return rc;
    if (!superuser(x) ||
        (actor && (u->flags & USER_SUPERUSER) && !(flags & USER_SUPERUSER) && users_superusers(us) == 1)) {
        unlock(us);
        return TSDB_ERR_DENIED;
    }
    if (u->flags == flags)
        return change_none(us, 0);
    rbuf b;
    change_buf(us, &b);
    rec_u8(&b, REC_FLAGS, u->uid, flags);
    return change_write(us, &b);
}

static int change_grant(users *us, uint32_t actor, const char *name, uint8_t kind, const char *sel, size_t len,
                        uint8_t rights, bool grant)
{
    if (rights < 1 || rights > R_EVERY || len > SEL_MAX)
        return TSDB_ERR_INVALID;
    user_grant s = {0};
    if (!(s.sel = malloc(len + 1)))
        return TSDB_ERR_NOMEM;
    size_t nl;
    int rc = sel_normalize(kind, sel, len, &s.kind, s.sel, &nl);
    if (rc == 0) {
        s.len = (uint16_t)nl;
        rc = s.kind == SEL_GLOB ? glob_parts(&s) : 0;
    }
    const user *x = NULL, *u = NULL;
    if (rc == 0)
        rc = change_target(us, actor, name, &x, &u);
    if (rc) {
        grant_clear(&s);
        return rc;
    }
    long gi = grant_find(u, s.kind, s.sel, s.len);
    uint8_t had = gi >= 0 ? u->g[gi].rights : 0, now;
    if (grant)
        now = had | rights;
    else if ((had & R_ADMIN) && !(rights & R_ADMIN) && (rights & R_EVERY & ~R_ADMIN))
        now = expand(had) & (uint8_t)~rights & (uint8_t)~R_ADMIN; /* admin implies what goes: it goes too (A23) */
    else
        now = had & (uint8_t)~rights; /* the rights admin implied go with it, unless held as such */
    if (grant ? !may_grant(x, u, &s) : !may_revoke(x, &s))
        rc = TSDB_ERR_DENIED;
    else if (now != had && gi < 0 && s.kind == SEL_GLOB && glob_count(u) >= USER_GLOBS)
        rc = TSDB_ERR_INVALID;
    else if (!grant && !(expand(had) & rights))
        /* nothing of it to revoke: said to whoever may know the user exists (decision 5), DENIED to anyone else,
         * as for a name that does not exist, so that a revoke of one's own selector lists no other tenant's users */
        rc = may_create(x, u->name, 0) ? TSDB_ERR_NOTFOUND : TSDB_ERR_DENIED;
    if (rc == TSDB_ERR_NOTFOUND || (rc == 0 && now == had)) {
        grant_clear(&s);
        return change_none(us, rc);
    }
    if (rc) {
        unlock(us);
        grant_clear(&s);
        return rc;
    }
    rbuf b;
    change_buf(us, &b);
    rec_grant(&b, u->uid, s.kind, now, s.sel, s.len);
    grant_clear(&s);
    return change_write(us, &b);
}

int users_grant(users *us, uint32_t actor, const char *name, uint8_t kind, const char *sel, size_t len,
                uint8_t rights)
{
    return change_grant(us, actor, name, kind, sel, len, rights, true);
}

int users_revoke(users *us, uint32_t actor, const char *name, uint8_t kind, const char *sel, size_t len,
                 uint8_t rights)
{
    return change_grant(us, actor, name, kind, sel, len, rights, false);
}

int users_compact(users *us)
{
    int rc = change_lock(us);
    if (rc)
        return rc;
    rbuf b;
    memset(&b, 0, sizeof b);
    uint8_t key[32];
    if (!us->have_header && tsdb_random(us->mock_key, 32) != 0)
        b.oom = true;
    memcpy(key, us->mock_key, 32);
    rec_header(&b, key);
    tsdb_wipe(key, sizeof key);
    for (uint32_t i = 0; i < us->n; i++) {
        const user *u = us->u[i];
        rec_user(&b, u->uid, u->flags, u->name);
        if (u->has_verifier)
            rec_verifier(&b, u->uid, &u->v);
        for (uint32_t j = 0; j < u->ng; j++)
            rec_grant(&b, u->uid, u->g[j].kind, u->g[j].rights, u->g[j].sel, u->g[j].len);
    }
    /* uids are never reused: when the highest one was dropped, a user of that uid is created and dropped again */
    if (us->max_uid && (!us->n || us->u[us->n - 1]->uid != us->max_uid)) {
        char name[32];
        for (uint32_t k = 0;; k++) {
            snprintf(name, sizeof name, "dropped-%u.%u", us->max_uid, k);
            if (!users_find(us, name))
                break;
        }
        rec_user(&b, us->max_uid, 0, name);
        rec_u8(&b, REC_DROP, us->max_uid, -1);
    }
    char tmp[4300];
    snprintf(tmp, sizeof tmp, "%s.compact", us->path);
    int fd = -1;
    if (b.oom) {
        rc = TSDB_ERR_NOMEM;
        goto out;
    }
    /* a new file of this process's own: whatever has the name (a dead compaction's file, or a link someone put
     * there) is unlinked first, the name itself and never what a link names, and O_EXCL does not follow one */
    int dfd = users_dirfd(us);
    if (dfd < 0 || (unlinkat(dfd, "USERS.compact", 0) != 0 && errno != ENOENT) ||
        (fd = open_in_dir(us, "USERS.compact", O_WRONLY | O_CREAT | O_EXCL, 0600)) < 0) {
        if (dfd >= 0 && errno != ELOOP) /* ELOOP: open_in_dir said why */
            fprintf(stderr, "tsdb: %s: %s\n", tmp, strerror(errno));
        rc = TSDB_ERR_IO;
        goto out;
    }
    for (size_t done = 0; done < b.n && rc == 0;) {
        ssize_t w = write(fd, b.p + done, b.n - done);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            rc = TSDB_ERR_IO;
        else
            done += (size_t)w;
    }
    struct stat st, d;
    /* errors before the rename fail just this call: nothing relies on the new file yet. The new file is locked
     * before its name is: a writer that opens the name after the rename waits until the directory fsync made the
     * rename durable (it would otherwise append to a file a power loss can take back, and acknowledge). */
    if (rc || fstat(dfd, &d) != 0 || (rc = file_perms(tmp, fd, &d)) != 0 || tsdb_fsync(fd) != 0 ||
        fstat(fd, &st) != 0 || lock_fd(fd, F_WRLCK) != 0 || renameat(dfd, "USERS.compact", dfd, "USERS") != 0) {
        rc = TSDB_ERR_IO;
        unlinkat(dfd, "USERS.compact", 0);
        goto out;
    }
    if (fsync_dir(us->dir) != 0) {
        users_fail(us, "the directory fsync after a compaction");
        rc = TSDB_ERR_IO;
        goto out;
    }
    memcpy(us->synced_id, b.p + 5 + 36, 16);
    memcpy(us->file_id, us->synced_id, 16);
    us->name_synced = true;
    /* the same state, now read from the new file; the old one's lock goes with its descriptor */
    us->ino = st.st_ino, us->dev = st.st_dev, us->off = b.n;
    uint8_t type;
    const uint8_t *pl;
    size_t len, rs;
    for (size_t pos = 0; parse_rec(b.p, pos, b.n, 0, &type, &pl, &len, &rs); pos += rs)
        us->last_off = pos, us->last_check = ld64(b.p + pos + rs - 8);
out:
    if (fd >= 0)
        close(fd);
    unlock(us);
    if (rc == 0) {
        close(us->fd);
        us->fd = -1;
    }
    free(b.p);
    return rc;
}
