// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Login (protocol 12, {@code docs/design-auth.md}): SCRAM-SHA-256 as the client (RFC 5802, RFC 7677), the verifiers
 * the admin calls send, and the process-wide key cache.
 *
 * <p>The password never travels (A18). A login proves it with HMACs over the exchange, and the client checks the
 * server's signature before the connect returns: a server that does not hold the user's verifier cannot pass.
 * PBKDF2 is the expensive step (4096 iterations or more, by design); its keys are cached per process by user, salt,
 * iterations and password, so every connection of the process, and every reconnect, pays for it once. Passwords
 * are their UTF-8 bytes, without SASLprep (A20), as in the other clients. The exchange is read strictly, as the C
 * client reads it.
 */
final class Scram {
    /** The iteration count of a verifier made without one, and the fewest a server may ask for at login: fewer would
     * be a downgrade a fake server could force on the client. */
    static final int ITERATIONS = 4096;
    /** More would let a fake server pin the client's CPU for minutes (as the C client). */
    static final int ITER_MAX = 100_000_000;
    private static final int SALT_LEN = 16;
    private static final int KEY_CACHE = 16;
    private static final SecureRandom RANDOM = new SecureRandom();

    private static final AtomicLong PBKDF2_RUNS = new AtomicLong();
    private static volatile String testNonce;

    private final String user;
    private final String nonce;
    private final String first; /* client-first-message with its gs2 header "n,," */

    /** Test hook: every later login of this process uses {@code nonce} as its client nonce; null: random ones. */
    static void setClientNonce(String nonce) {
        testNonce = nonce;
    }

    /** Test hook: how many times this process ran PBKDF2 (logins and verifiers). */
    static long pbkdf2Runs() {
        return PBKDF2_RUNS.get();
    }

    /** Starts a login of {@code user}: the client-first-message, sent in AUTH_START. */
    Scram(String user) {
        /* user names never hold ',' or '=' (USERS refuses them), so n= needs no escaping */
        int len = user.getBytes(StandardCharsets.UTF_8).length;
        if (len == 0 || len > 255 || user.indexOf(',') >= 0 || user.indexOf('=') >= 0 || user.indexOf('\0') >= 0)
            throw new IllegalArgumentException("user name \"" + user + "\": 1 to 255 bytes without , = or NUL");
        String n = testNonce;
        if (n == null) {
            byte[] r = new byte[18];
            RANDOM.nextBytes(r);
            n = Base64.getEncoder().encodeToString(r);
        }
        this.user = user;
        this.nonce = n;
        this.first = "n,,n=" + user + ",r=" + n;
    }

    /** AUTH_START's payload: u8 len, mechanism, u16 len, client-first-message. */
    byte[] startBody() {
        byte[] mech = Protocol.SCRAM_SHA256.getBytes(StandardCharsets.US_ASCII);
        byte[] f = first.getBytes(StandardCharsets.UTF_8);
        byte[] b = new byte[1 + mech.length + 2 + f.length];
        b[0] = (byte) mech.length;
        System.arraycopy(mech, 0, b, 1, mech.length);
        b[1 + mech.length] = (byte) f.length;
        b[2 + mech.length] = (byte) (f.length >>> 8);
        System.arraycopy(f, 0, b, 3 + mech.length, f.length);
        return b;
    }

    /** The client's answer to the server-first-message: AUTH_CONTINUE's payload and the expected ServerSignature. */
    record Final(byte[] body, byte[] serverSignature) {
    }

    /**
     * From the server-first-message ({@code r=<nonce>,s=<salt>,i=<iterations>}, nothing else): AUTH_CONTINUE's
     * payload, and the ServerSignature the server must answer with.
     */
    Final finish(byte[] password, byte[] serverFirst) throws TsdbException {
        for (byte c : serverFirst) /* printable ASCII throughout: a nonce, base64 and digits */
            if (c < 0x21 || c > 0x7e)
                throw bad();
        String sf = new String(serverFirst, StandardCharsets.US_ASCII);
        if (!sf.startsWith("r="))
            throw bad();
        int s = sf.indexOf(",s="), i = s < 0 ? -1 : sf.indexOf(",i=", s + 3);
        if (s < 0 || i < 0)
            throw bad();
        String snonce = sf.substring(2, s), salt64 = sf.substring(s + 3, i), iter = sf.substring(i + 3);
        /* the server's nonce continues ours, without ',' */
        if (snonce.length() <= nonce.length() || !snonce.startsWith(nonce) || snonce.indexOf(',') >= 0)
            throw bad();
        byte[] salt = unbase64(salt64);
        if (salt == null || salt.length == 0 || salt.length > 64)
            throw bad();
        if (iter.isEmpty() || iter.charAt(0) == '0' || !iter.chars().allMatch(c -> c >= '0' && c <= '9'))
            throw bad();
        long it = iter.length() > 10 ? Long.MAX_VALUE : Long.parseLong(iter);
        if (it < ITERATIONS || it > ITER_MAX)
            throw refused("the server asks for an iteration count outside 4096 to 100,000,000");
        byte[][] k = keysCached(user, password, salt, (int) it);
        byte[] ck = k[0], sk = k[1];
        /* AuthMessage = client-first-message-bare "," server-first-message "," client-final-message-without-proof */
        String withoutProof = "c=biws,r=" + snonce;
        byte[] am = (first.substring(3) + "," + sf + "," + withoutProof).getBytes(StandardCharsets.UTF_8);
        byte[] sig = hmac(sha256(ck), am);
        byte[] proof = new byte[32];
        for (int j = 0; j < 32; j++)
            proof[j] = (byte) (ck[j] ^ sig[j]);
        byte[] serverSig = hmac(sk, am);
        Arrays.fill(ck, (byte) 0);
        Arrays.fill(sk, (byte) 0);
        byte[] msg = (withoutProof + ",p=" + Base64.getEncoder().encodeToString(proof))
                .getBytes(StandardCharsets.US_ASCII);
        Arrays.fill(proof, (byte) 0);
        byte[] body = new byte[2 + msg.length];
        body[0] = (byte) msg.length;
        body[1] = (byte) (msg.length >>> 8);
        System.arraycopy(msg, 0, body, 2, msg.length);
        return new Final(body, serverSig);
    }

    /**
     * Checks the server-final-message: {@code v=} and the ServerSignature, compared in constant time. An {@code e=}
     * (or anything else) fails: the server did not prove it holds this user's verifier.
     */
    static void checkServerFinal(byte[] msg, byte[] expected) throws TsdbException {
        byte[] v = null;
        if (msg.length >= 2 && msg[0] == 'v' && msg[1] == '=')
            v = unbase64(new String(msg, 2, msg.length - 2, StandardCharsets.ISO_8859_1));
        if (v == null || !MessageDigest.isEqual(v, expected))
            throw refused("the server's signature is wrong: it does not hold this user's verifier");
    }

    /**
     * A password's verifier with a fresh salt (A18): u32 iterations, u8 salt_len, salt, StoredKey, ServerKey.
     *
     * @param iterations PBKDF2 iterations, 4096 to 100,000,000
     */
    static byte[] verifier(byte[] password, int iterations) {
        checkPassword(password);
        return verifierOf(password, iterations);
    }

    /** The password rule of every client and the CLI (design-auth.md §6): 1 to PASSWORD_MAX bytes, no NUL. */
    static void checkPassword(byte[] pw) {
        boolean nul = false;
        for (byte b : pw)
            nul |= b == 0;
        if (pw.length == 0 || pw.length > ConnectOptions.PASSWORD_MAX || nul)
            throw new IllegalArgumentException(
                    "a password is 1 to " + ConnectOptions.PASSWORD_MAX + " bytes without NUL");
    }

    /**
     * The UTF-8 bytes of a password. An unpaired surrogate is refused, not replaced by '?' as String.getBytes and
     * Charset.encode do: the password stored would not be the one typed, and no other client could log in with it.
     */
    static byte[] utf8(char[] password) {
        java.nio.ByteBuffer b;
        try {
            b = StandardCharsets.UTF_8.newEncoder().onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(password));
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new IllegalArgumentException("a password that is not valid UTF-16 (an unpaired surrogate)");
        }
        byte[] pw = new byte[b.remaining()];
        b.get(pw);
        if (b.hasArray())
            Arrays.fill(b.array(), (byte) 0);
        return pw;
    }

    private static byte[] verifierOf(byte[] password, int iterations) {
        if (iterations < ITERATIONS || iterations > ITER_MAX)
            throw new IllegalArgumentException(iterations + " iterations: 4096 to 100,000,000");
        byte[] salt = new byte[SALT_LEN];
        RANDOM.nextBytes(salt);
        byte[][] k = keys(password, salt, iterations);
        byte[] v = new byte[4 + 1 + SALT_LEN + 64];
        v[0] = (byte) iterations;
        v[1] = (byte) (iterations >>> 8);
        v[2] = (byte) (iterations >>> 16);
        v[3] = (byte) (iterations >>> 24);
        v[4] = SALT_LEN;
        System.arraycopy(salt, 0, v, 5, SALT_LEN);
        System.arraycopy(sha256(k[0]), 0, v, 5 + SALT_LEN, 32);
        System.arraycopy(k[1], 0, v, 5 + SALT_LEN + 32, 32);
        Arrays.fill(k[0], (byte) 0);
        Arrays.fill(k[1], (byte) 0);
        return v;
    }

    private static TsdbException bad() {
        return refused("the server's first message does not follow SCRAM-SHA-256");
    }

    static TsdbException refused(String why) {
        return new TsdbException(Status.ERR_AUTH, why, null);
    }

    /** Strict base64: whole quads, padding only at the end, nothing outside the alphabet. */
    static byte[] unbase64(String s) {
        if (s.length() % 4 != 0)
            return null;
        try {
            return Base64.getDecoder().decode(s); /* refuses other letters and '=' inside */
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // ------------------------------------------------------------ crypto

    private static Mac mac(byte[] key) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            /* SecretKeySpec refuses an empty key; HMAC pads any key, so an empty one is 64 zero bytes */
            m.init(new SecretKeySpec(key.length > 0 ? key : new byte[64], "HmacSHA256"));
            return m;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is part of every Java runtime", e);
        }
    }

    static byte[] hmac(byte[] key, byte[] msg) {
        return mac(key).doFinal(msg);
    }

    static byte[] sha256(byte[] b) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(b);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 is part of every Java runtime", e);
        }
    }

    /** PBKDF2-HMAC-SHA-256 with one 32-byte block: SaltedPassword of RFC 5802. */
    static byte[] pbkdf2(byte[] password, byte[] salt, int iterations) {
        PBKDF2_RUNS.incrementAndGet();
        Mac m = mac(password); /* doFinal resets it to the keyed state: the key's pads are set up once */
        m.update(salt);
        m.update(new byte[] {0, 0, 0, 1});
        byte[] u = m.doFinal();
        byte[] t = u.clone();
        for (int i = 1; i < iterations; i++) {
            u = m.doFinal(u);
            for (int j = 0; j < t.length; j++)
                t[j] ^= u[j];
        }
        Arrays.fill(u, (byte) 0);
        return t;
    }

    /** ClientKey and ServerKey of a password. */
    private static byte[][] keys(byte[] password, byte[] salt, int iterations) {
        byte[] salted = pbkdf2(password, salt, iterations);
        byte[][] k = {hmac(salted, "Client Key".getBytes(StandardCharsets.US_ASCII)),
            hmac(salted, "Server Key".getBytes(StandardCharsets.US_ASCII))};
        Arrays.fill(salted, (byte) 0);
        return k;
    }

    // ------------------------------------------------------------ key cache

    /** One key; clientKey and serverKey are null while one login computes them and others wait for it. */
    private static final class KeyEntry {
        final String user;
        final byte[] salt, pwMac;
        int iterations; // 0 once a computation failed: it matches no login (they ask for 4096 or more)
        byte[] clientKey, serverKey;

        KeyEntry(String user, byte[] salt, int iterations, byte[] pwMac) {
            this.user = user;
            this.salt = salt;
            this.iterations = iterations;
            this.pwMac = pwMac;
        }

        boolean pending() {
            return clientKey == null;
        }
    }

    private static final KeyEntry[] CACHE = new KeyEntry[KEY_CACHE];
    private static int cacheNext;

    /**
     * ClientKey and ServerKey (copies the caller clears), from the cache or computed once. HMAC(salt, password)
     * tells one password from another without keeping it. The connections of a pool, or the threads of a sink,
     * start together: the first to miss claims an entry and computes it outside the lock, the others wait for it
     * (one PBKDF2 per key, not one per connection, and one entry, so other users' keys are not evicted by
     * copies). A pending entry is never evicted; with KEY_CACHE keys in flight, one more is computed uncached.
     */
    private static byte[][] keysCached(String user, byte[] password, byte[] salt, int iterations) {
        byte[] pwMac = hmac(salt, password);
        KeyEntry mine = null;
        synchronized (CACHE) {
            boolean interrupted = false;
            for (;;) {
                KeyEntry e = find(user, salt, iterations, pwMac);
                if (e == null)
                    break;
                if (!e.pending())
                    return new byte[][] {e.clientKey.clone(), e.serverKey.clone()};
                try {
                    CACHE.wait(); // another login computes this key
                } catch (InterruptedException ie) {
                    interrupted = true; // compute it here, uncached; the caller still sees the interrupt
                    break;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            for (int k = 0; k < KEY_CACHE && mine == null && !interrupted; k++) { // the next slot nobody waits for
                int i = Math.floorMod(cacheNext++, KEY_CACHE);
                KeyEntry old = CACHE[i];
                if (old == null || !old.pending()) {
                    if (old != null) {
                        Arrays.fill(old.clientKey, (byte) 0);
                        Arrays.fill(old.serverKey, (byte) 0);
                    }
                    mine = CACHE[i] = new KeyEntry(user, salt.clone(), iterations, pwMac);
                }
            }
        }
        byte[][] k = null;
        try {
            k = keys(password, salt, iterations);
            return k;
        } finally {
            if (mine != null) {
                synchronized (CACHE) {
                    if (k != null) {
                        mine.clientKey = k[0].clone();
                        mine.serverKey = k[1].clone();
                    } else { // it threw: the waiters compute it themselves
                        mine.iterations = 0;
                        mine.clientKey = mine.serverKey = new byte[32];
                    }
                    CACHE.notifyAll();
                }
            }
        }
    }

    private static KeyEntry find(String user, byte[] salt, int iterations, byte[] pwMac) {
        for (KeyEntry e : CACHE)
            if (e != null && e.iterations == iterations && e.user.equals(user) && Arrays.equals(e.salt, salt)
                    && MessageDigest.isEqual(e.pwMac, pwMac))
                return e;
        return null;
    }
}
