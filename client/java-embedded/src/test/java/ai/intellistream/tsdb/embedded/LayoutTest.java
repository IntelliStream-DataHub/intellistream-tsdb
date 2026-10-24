package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.ValueType;

import java.io.IOException;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.PaddingLayout;
import java.lang.foreign.StructLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The struct layouts of {@link Native} have the sizes and offsets the C compiler gives tsdb.h's structs. */
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
        String include = System.getProperty("tsdb.include", Paths.get("../../include").toAbsolutePath().toString());
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
        for (StructLayout s : Native.ALL_STRUCTS) {
            String name = s.name().orElseThrow();
            assertEquals(c.get(name), s.byteSize(), name + " size");
            checked.add(name);
            for (MemoryLayout m : s.memberLayouts()) {
                if (m instanceof PaddingLayout)
                    continue;
                String field = name + "." + m.name().orElseThrow();
                assertEquals(c.get(field), Native.offset(s, m.name().get()), field + " offset");
                checked.add(field);
                assertTrue(!(m instanceof GroupLayout), "flat structs only");
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
        assertEquals(0L, c.get("order.ASC"));
        assertEquals(1L, c.get("order.DESC"));
        assertEquals(5, c.keySet().stream().filter(k -> k.startsWith("type.")).count(), "a C type Java lacks");
        assertEquals(8, c.keySet().stream().filter(k -> k.startsWith("unit.")).count(), "a C unit Java lacks");
        for (int i = 0; i < Native.ALL_STRUCTS.length; i++)
            assertEquals(c.get("kind." + Native.ALL_STRUCTS[i].name().orElseThrow()), (long) i,
                    "tsdb_struct_kind of " + Native.ALL_STRUCTS[i].name().orElseThrow());
    }

    @Test
    void aLibraryWithOtherStructSizesIsRefused() {
        Native.checkAbi("test", k -> Native.ALL_STRUCTS[k].byteSize()); /* the loaded library passed this too */
        for (int bad = 0; bad < Native.ALL_STRUCTS.length; bad++) {
            int which = bad;
            UnsatisfiedLinkError e = assertThrows(UnsatisfiedLinkError.class, () -> Native.checkAbi("old.so",
                    k -> Native.ALL_STRUCTS[k].byteSize() + (k == which ? 8 : 0)));
            assertTrue(e.getMessage().contains(Native.ALL_STRUCTS[which].name().orElseThrow()), e.getMessage());
        }
    }
}
