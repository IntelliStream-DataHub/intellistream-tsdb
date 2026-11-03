package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.TwaMethod;
import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.ValueType;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.PaddingLayout;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The struct layouts of {@link Native} have the sizes and offsets the C compiler gives tsdb.h's structs, list every
 * field tsdb.h declares, and every downcall handle and callback has the signature of its C prototype.
 */
class LayoutTest {
    @TempDir
    Path tmp;

    /* The compiler and the layout probe are not under test: a sanitizer preloaded into this JVM (the leak check runs
     * the tests with LD_PRELOAD=libasan.so) would otherwise fail cc on cc1's own leaks. */
    private static ProcessBuilder plain(ProcessBuilder pb) {
        pb.environment().keySet().removeIf(k -> k.equals("LD_PRELOAD") || k.endsWith("SAN_OPTIONS"));
        return pb.redirectErrorStream(true);
    }

    @Test
    void layoutsMatchTheCompiler() throws IOException, InterruptedException {
        Path src = Paths.get("src/test/c/layouts.c").toAbsolutePath();
        String include = include();
        Path bin = tmp.resolve("layouts");
        Process cc = plain(new ProcessBuilder("cc", "-std=gnu11", "-I" + include, "-o", bin.toString(), src.toString()))
                .start();
        String ccOut = new String(cc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, cc.waitFor(), ccOut);
        Process run = plain(new ProcessBuilder(bin.toString())).start();
        String out = new String(run.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, run.waitFor(), out);
        Map<String, Long> c = new HashMap<>();
        for (String line : out.split("\n")) {
            String[] kv = line.trim().split(" ");
            c.put(kv[0], Long.parseLong(kv[1]));
        }
        assertEquals(4L, c.get("enum"), "enums are 4 bytes");
        Set<String> checked = new HashSet<>();
        for (StructLayout s : structs()) {
            String name = s.name().orElseThrow();
            assertEquals(c.get(name), s.byteSize(), name + " size");
            checked.add(name);
            for (MemoryLayout m : s.memberLayouts()) {
                if (m instanceof PaddingLayout)
                    continue;
                String field = name + "." + m.name().orElseThrow();
                assertEquals(c.get(field), Native.offset(s, m.name().get()), field + " offset");
                checked.add(field);
                /* flat structs, but for structs of their own embedded whole (tsdb_spread_bucket.b,
                 * tsdb_multi_query.opts and .twa, tsdb_series_result.values), whose fields are checked as theirs */
                assertTrue(!(m instanceof GroupLayout) || m.equals(Native.BUCKET.withName(m.name().get()))
                        || m.equals(Native.QUERY_OPTS.withName(m.name().get()))
                        || m.equals(Native.TWA_OPTS.withName(m.name().get()))
                        || m.equals(Native.VALUES.withName(m.name().get())), field + ": flat structs only");
            }
        }
        for (String k : c.keySet())
            assertTrue(k.equals("enum") || k.contains(".") && !k.startsWith("tsdb_") || checked.contains(k),
                    k + " is not laid out in Native");
        /* the codes the binding passes are the C enums' values, not ordinals that happen to match today */
        for (ValueType t : ValueType.values())
            assertEquals(c.get("type." + t.name()), (long) t.code(), "tsdb_type of " + t);
        for (Unit u : Unit.values())
            assertEquals(c.get("unit." + u.name()), (long) u.code(), "tsdb_unit of " + u);
        for (TwaMethod t : TwaMethod.values())
            assertEquals(c.get("twa." + t.name()), (long) t.ordinal(), "tsdb_twa_method of " + t + " (Native.twaOpts)");
        assertEquals(TwaMethod.values().length, c.keySet().stream().filter(k -> k.startsWith("twa.")).count());
        assertEquals((long) Native.MULTI_VALUES, c.get("multi.VALUES"));
        assertEquals((long) Native.MULTI_BUCKETS, c.get("multi.BUCKETS"));
        assertEquals((long) Native.MULTI_SPREAD, c.get("multi.SPREAD"));
        assertEquals((long) Native.MULTI_TWA, c.get("multi.TWA"));
        assertEquals((long) Native.MULTI_LTTB, c.get("multi.LTTB"));
        assertEquals((long) Native.MULTI_TRIM_LO, c.get("multi.TRIM_LO"));
        assertEquals((long) Native.MULTI_TRIM_HI, c.get("multi.TRIM_HI"));
        assertEquals((long) Native.ABI_VERSION, c.get("abi.VERSION"), "TSDB_ABI_VERSION: the binding is out of date");
        assertEquals(0L, c.get("order.ASC"));
        assertEquals(1L, c.get("order.DESC"));
        assertEquals(5, c.keySet().stream().filter(k -> k.startsWith("type.")).count(), "a C type Java lacks");
        assertEquals(8, c.keySet().stream().filter(k -> k.startsWith("unit.")).count(), "a C unit Java lacks");
        for (int i = 0; i < Native.ALL_STRUCTS.length; i++)
            assertEquals(c.get("kind." + Native.ALL_STRUCTS[i].name().orElseThrow()), (long) i,
                    "tsdb_struct_kind of " + Native.ALL_STRUCTS[i].name().orElseThrow());
    }

    /** Every struct Native lays out; each has a tsdb_struct_kind. */
    private static List<StructLayout> structs() {
        return List.of(Native.ALL_STRUCTS);
    }

    private static String include() {
        return System.getProperty("tsdb.include", Paths.get("../../include").toAbsolutePath().toString());
    }

    /** tsdb.h and tsdb_calendar.h without comments and preprocessor lines. */
    private static String headers() throws IOException {
        StringBuilder b = new StringBuilder();
        for (String h : new String[] {"tsdb.h", "tsdb_calendar.h"})
            b.append(Files.readString(Paths.get(include(), h), StandardCharsets.UTF_8)).append('\n');
        String s = b.toString().replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("//[^\n]*", "");
        return s.replaceAll("(?m)^\\s*#.*$", "");
    }

    /**
     * The offsets above cover only the fields Native lists. A field the header gained (in the middle, at the end, or
     * in what was padding, where the size stays the same) must be laid out too, or the engine reads or writes past
     * what Java allocated, or reads padding Java never sets.
     */
    @Test
    void everyFieldOfTheHeaderIsLaidOut() throws IOException {
        String h = headers();
        for (StructLayout s : structs()) {
            String name = s.name().orElseThrow();
            Matcher m = Pattern.compile("typedef struct " + name + " \\{(.*?)\\} " + name + ";", Pattern.DOTALL)
                    .matcher(h);
            assertTrue(m.find(), name + " not found in tsdb.h");
            Set<String> java = new HashSet<>();
            for (MemoryLayout l : s.memberLayouts())
                l.name().ifPresent(java::add);
            List<String> fields = new ArrayList<>();
            for (String decl : m.group(1).split(";")) {
                decl = decl.trim();
                if (decl.isEmpty())
                    continue;
                int fp = decl.indexOf("(*");
                if (fp >= 0) { /* a function pointer: int64_t (*name)(args) */
                    fields.add(decl.substring(fp + 2, decl.indexOf(')', fp)).trim());
                    continue;
                }
                decl = decl.replaceAll("\\[[^\\]]*\\]", ""); /* char name[256] */
                String first = decl.split(",")[0];
                int cut = Math.max(first.lastIndexOf('*'), Math.max(first.lastIndexOf(' '), first.lastIndexOf('\t')));
                for (String n : decl.substring(cut + 1).split(","))
                    fields.add(n.trim().replace("*", ""));
            }
            assertTrue(fields.size() >= 2, name + ": parsed " + fields);
            for (String f : fields)
                assertTrue(java.contains(f), name + "." + f + " is in tsdb.h but not laid out in Native");
            assertEquals(fields.size(), java.size(), name + ": Native has fields tsdb.h lacks: " + java);
        }
    }

    /** The Java carrier of a C parameter or return type; null for void. */
    private static Class<?> carrier(String decl, Map<String, Class<?>> typedefs) {
        decl = decl.trim();
        if (decl.contains("*"))
            return MemorySegment.class;
        String[] words = decl.replaceAll("\\bconst\\b", " ").trim().split("\\s+");
        switch (words[0]) {
            case "void": return null;
            case "int64_t": case "uint64_t": case "size_t": return long.class;
            case "int": case "int32_t": case "uint32_t": return int.class;
            case "float": return float.class;
            case "double": return double.class;
            default:
                if (typedefs.containsKey(words[0]))
                    return typedefs.get(words[0]);
                throw new AssertionError("no Java carrier for the C type of \"" + decl + "\"");
        }
    }

    /** {@code params} split at the commas outside parentheses (a function-pointer parameter has its own). */
    private static List<String> params(String params) {
        List<String> out = new ArrayList<>();
        int depth = 0, from = 0;
        for (int i = 0; i < params.length(); i++) {
            char ch = params.charAt(i);
            if (ch == '(')
                depth++;
            else if (ch == ')')
                depth--;
            else if (ch == ',' && depth == 0) {
                out.add(params.substring(from, i));
                from = i + 1;
            }
        }
        out.add(params.substring(from));
        if (out.size() == 1 && out.get(0).trim().equals("void"))
            out.clear();
        return out;
    }

    private static MethodType methodType(String ret, String params, Map<String, Class<?>> enums) {
        Class<?> r = carrier(ret, enums);
        List<Class<?>> ps = new ArrayList<>();
        for (String p : params(params))
            ps.add(carrier(p, enums));
        return MethodType.methodType(r == null ? void.class : r, ps);
    }

    /** The text inside the parentheses that open at {@code open}. */
    private static String inside(String s, int open) {
        int depth = 0;
        for (int i = open; i < s.length(); i++) {
            if (s.charAt(i) == '(')
                depth++;
            else if (s.charAt(i) == ')' && --depth == 0)
                return s.substring(open + 1, i);
        }
        throw new AssertionError("unbalanced parentheses after " + s.substring(open, Math.min(s.length(), open + 40)));
    }

    /**
     * A function whose C signature changed (an argument added, an int widened to int64_t) still links, and then
     * reads garbage or misses an argument: the handles are compared with the prototypes here, the callbacks with
     * their typedefs. Signedness is not compared (FFM has no unsigned carriers).
     */
    @Test
    void handlesMatchThePrototypes() throws IOException {
        String h = headers().replaceAll("\\s+", " ");
        Map<String, Class<?>> enums = new HashMap<>(); /* and function pointer types: the typedefs by value */
        Matcher e = Pattern.compile("typedef enum (?:\\w+ )?\\{[^}]*\\} (\\w+);").matcher(h);
        while (e.find())
            enums.put(e.group(1), int.class);
        assertTrue(enums.keySet().containsAll(List.of("tsdb_type", "tsdb_unit", "tsdb_order")), "enums " + enums);
        e = Pattern.compile("typedef [^;()]*\\( ?\\* ?(\\w+) ?\\) ?\\(").matcher(h);
        while (e.find())
            enums.put(e.group(1), MemorySegment.class);
        e = Pattern.compile("typedef ((?:u?int(?:8|16|32|64)_t|size_t|int|float|double)) (\\w+);").matcher(h);
        while (e.find()) /* scalar typedefs: tsdb_sid */
            enums.put(e.group(2), carrier(e.group(1), enums));
        assertEquals(long.class, enums.get("tsdb_sid"), "series ids are 64 bits");
        /* callbacks declared inline in a prototype, the second parameter of each */
        Map<String, FunctionDescriptor> inline = Map.of("tsdb_foreach_part", Native.PART_FN, "tsdb_series_foreach",
                Native.SERIES_FN);
        assertTrue(Native.BOUND.keySet().containsAll(inline.keySet()), "bound " + Native.BOUND.keySet());
        assertTrue(Native.BOUND.size() >= 40, "bound " + Native.BOUND.keySet());
        for (Map.Entry<String, FunctionDescriptor> b : Native.BOUND.entrySet()) {
            String name = b.getKey();
            Matcher m = Pattern.compile("(?:^|[;{}])\\s*([^;{}()]*?[\\s*])" + name + " ?\\(").matcher(h);
            assertTrue(m.find(), name + ": no prototype in tsdb.h or tsdb_calendar.h");
            String args = inside(h, m.end() - 1);
            assertEquals(methodType(m.group(1), args, enums), b.getValue().toMethodType(), name + "(" + args + ")");
            if (inline.containsKey(name)) {
                String fn = params(args).get(1);
                Matcher r = Pattern.compile("^\\s*(.*?)\\(\\s*\\*\\s*\\w+\\s*\\)\\s*\\(").matcher(fn);
                assertTrue(r.find(), fn);
                assertEquals(methodType(r.group(1), inside(fn, r.end() - 1), enums), inline.get(name).toMethodType(),
                        "the callback of " + name);
            }
        }
        for (Map.Entry<String, FunctionDescriptor> cb : Map.of("tsdb_raw_fn", Native.RAW_FN, "tsdb_values_fn",
                Native.VALUES_FN).entrySet()) {
            Matcher m = Pattern.compile("typedef ([^;()]*)\\( ?\\* ?" + cb.getKey() + " ?\\) ?\\(").matcher(h);
            assertTrue(m.find(), cb.getKey() + " not found");
            assertEquals(methodType(m.group(1), inside(h, m.end() - 1), enums), cb.getValue().toMethodType(),
                    cb.getKey());
        }
    }

    @Test
    void aLibraryWithOtherStructSizesIsRefused() {
        Native.checkAbi("test", Native.ABI_VERSION, k -> Native.ALL_STRUCTS[k].byteSize()); /* as the loaded one */
        for (int bad = 0; bad < Native.ALL_STRUCTS.length; bad++) {
            int which = bad;
            UnsatisfiedLinkError e = assertThrows(UnsatisfiedLinkError.class, () -> Native.checkAbi("old.so",
                    Native.ABI_VERSION, k -> Native.ALL_STRUCTS[k].byteSize() + (k == which ? 8 : 0)));
            assertTrue(e.getMessage().contains(Native.ALL_STRUCTS[which].name().orElseThrow()), e.getMessage());
        }
    }

    /**
     * A library of another ABI version is refused even where every struct has the size this binding expects:
     * tsdb_series_result kept its 72 bytes when its series id became 64 bits and status and last moved.
     */
    @Test
    void aLibraryOfAnotherAbiVersionIsRefused() {
        assertEquals(2, Native.ABI_VERSION);
        assertEquals(Native.ABI_VERSION, Native.LIBRARY_ABI_VERSION, "the loaded library's tsdb_abi_version()");
        for (int other : new int[] {Native.ABI_VERSION - 1, Native.ABI_VERSION + 1}) {
            UnsatisfiedLinkError e = assertThrows(UnsatisfiedLinkError.class,
                    () -> Native.checkAbi("other.so", other, k -> Native.ALL_STRUCTS[k].byteSize()));
            assertTrue(e.getMessage().contains("other.so") && e.getMessage().contains("ABI version " + other)
                    && e.getMessage().contains("written for ABI version " + Native.ABI_VERSION), e.getMessage());
        }
    }
}
