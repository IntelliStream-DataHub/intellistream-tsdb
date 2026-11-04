/* tsdb site: theme, navigation drawer, language tabs, copy buttons and the benchmark charts.
 * Plain JavaScript, no libraries. Loaded in <head> without defer so the saved theme applies before the first
 * paint; everything else waits for the document. localStorage may be missing or throw (private windows,
 * blocked storage): every access is wrapped, and the page works without it. */
(function () {
    "use strict";

    var THEME_KEY = "tsdb-theme";
    var LANG_KEY = "tsdb-lang";
    var root = document.documentElement;

    function load(key) {
        try {
            return window.localStorage.getItem(key);
        } catch (e) {
            return null;
        }
    }

    function save(key, value) {
        try {
            window.localStorage.setItem(key, value);
        } catch (e) {
            /* not stored: the choice lasts for this page only */
        }
    }

    var savedTheme = load(THEME_KEY);
    if (savedTheme === "light" || savedTheme === "dark") root.setAttribute("data-theme", savedTheme);
    root.classList.remove("no-js");

    var reduceMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    /* ---------- theme ---------- */

    function effectiveTheme() {
        var t = root.getAttribute("data-theme");
        if (t) return t;
        return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }

    function initTheme() {
        var btn = document.querySelector(".theme-btn");
        if (!btn) return;
        function label() {
            var next = effectiveTheme() === "dark" ? "light" : "dark";
            btn.setAttribute("aria-label", "Switch to the " + next + " theme");
            btn.setAttribute("title", "Switch to the " + next + " theme");
        }
        label();
        btn.addEventListener("click", function () {
            var next = effectiveTheme() === "dark" ? "light" : "dark";
            root.setAttribute("data-theme", next);
            save(THEME_KEY, next);
            label();
        });
    }

    /* ---------- app bar and drawer ---------- */

    function initBar() {
        var bar = document.querySelector(".app-bar");
        if (!bar) return;
        function onScroll() {
            bar.classList.toggle("raised", window.scrollY > 4);
        }
        window.addEventListener("scroll", onScroll, { passive: true });
        onScroll();
    }

    function initDrawer() {
        var btn = document.querySelector(".menu-btn");
        var drawer = document.querySelector(".drawer");
        var scrim = document.querySelector(".scrim");
        if (!btn || !drawer) return;
        function set(open) {
            document.body.classList.toggle("drawer-open", open);
            btn.setAttribute("aria-expanded", open ? "true" : "false");
            if (open) {
                var first = drawer.querySelector("a");
                if (first) first.focus();
            }
        }
        btn.addEventListener("click", function () {
            set(!document.body.classList.contains("drawer-open"));
        });
        if (scrim) scrim.addEventListener("click", function () { set(false); });
        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape" && document.body.classList.contains("drawer-open")) {
                set(false);
                btn.focus();
            }
        });
        drawer.addEventListener("click", function (e) {
            if (e.target.closest("a")) set(false);
        });
    }

    /* ---------- language tabs ---------- */

    var tabGroups = [];

    function selectTab(group, key, focus) {
        var tabs = group.querySelectorAll('[role="tab"]');
        var found = false;
        for (var i = 0; i < tabs.length; i++) if (tabs[i].getAttribute("data-key") === key) found = true;
        if (!found) key = tabs[0].getAttribute("data-key");
        for (var j = 0; j < tabs.length; j++) {
            var on = tabs[j].getAttribute("data-key") === key;
            tabs[j].setAttribute("aria-selected", on ? "true" : "false");
            tabs[j].tabIndex = on ? 0 : -1;
            var panel = document.getElementById(tabs[j].getAttribute("aria-controls"));
            if (panel) panel.hidden = !on;
            if (on && focus) tabs[j].focus();
        }
    }

    function chooseLanguage(key, from, focus) {
        save(LANG_KEY, key);
        for (var i = 0; i < tabGroups.length; i++) selectTab(tabGroups[i], key, focus && tabGroups[i] === from);
    }

    function initTabs() {
        var groups = document.querySelectorAll(".tabs");
        var n = 0;
        for (var g = 0; g < groups.length; g++) {
            var group = groups[g];
            var tabs = group.querySelectorAll('[role="tab"]');
            var panels = group.querySelectorAll('[role="tabpanel"]');
            if (!tabs.length) continue;
            for (var t = 0; t < tabs.length; t++) {
                var key = tabs[t].getAttribute("data-key");
                var panel = null;
                for (var p = 0; p < panels.length; p++)
                    if (panels[p].getAttribute("data-key") === key) panel = panels[p];
                n++;
                tabs[t].id = "tab-" + n;
                if (panel) {
                    panel.id = "panel-" + n;
                    panel.setAttribute("aria-labelledby", tabs[t].id);
                    panel.tabIndex = 0;
                    tabs[t].setAttribute("aria-controls", panel.id);
                }
                tabs[t].addEventListener("click", function (e) {
                    var grp = e.currentTarget.closest(".tabs");
                    chooseLanguage(e.currentTarget.getAttribute("data-key"), grp, false);
                });
            }
            group.querySelector('[role="tablist"]').addEventListener("keydown", function (e) {
                var list = Array.prototype.slice.call(e.currentTarget.querySelectorAll('[role="tab"]'));
                var at = list.indexOf(document.activeElement);
                if (at < 0) return;
                var to = -1;
                if (e.key === "ArrowRight") to = (at + 1) % list.length;
                else if (e.key === "ArrowLeft") to = (at - 1 + list.length) % list.length;
                else if (e.key === "Home") to = 0;
                else if (e.key === "End") to = list.length - 1;
                if (to < 0) return;
                e.preventDefault();
                chooseLanguage(list[to].getAttribute("data-key"), e.currentTarget.closest(".tabs"), true);
            });
            tabGroups.push(group);
        }
        var start = load(LANG_KEY) || "c";
        for (var k = 0; k < tabGroups.length; k++) selectTab(tabGroups[k], start, false);
    }

    /* ---------- copy buttons ---------- */

    function initCopy() {
        var blocks = document.querySelectorAll("pre > code");
        for (var i = 0; i < blocks.length; i++) {
            var pre = blocks[i].parentNode;
            var wrap = document.createElement("div");
            wrap.className = "code-wrap";
            pre.parentNode.insertBefore(wrap, pre);
            wrap.appendChild(pre);
            var btn = document.createElement("button");
            btn.type = "button";
            btn.className = "copy";
            btn.textContent = "Copy";
            btn.setAttribute("aria-label", "Copy the code");
            wrap.appendChild(btn);
            btn.addEventListener("click", copy);
        }
    }

    function copy(e) {
        var btn = e.currentTarget;
        var text = btn.parentNode.querySelector("code").textContent;
        function done(ok) {
            btn.textContent = ok ? "Copied" : "Select and copy";
            setTimeout(function () { btn.textContent = "Copy"; }, 1600);
        }
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(function () { done(true); }, function () { done(false); });
        } else {
            done(false);
        }
    }

    /* ---------- charts ----------
     * Each <figure class="chart"> holds a <table class="chart-data">: the numbers, which stay on the page as the
     * text version. The script draws an SVG from that table.
     *   data-kind="bars"    one bar per row: <th> the system, <td data-v> its number
     *   data-kind="groups"  one group per row (a question), one bar per column (a system), each group scaled
     *                       to its own largest number
     * A <td data-fail="short reason"> is a failure: drawn as a marked box with the reason, never as a bar.
     * A <td data-na="short reason"> was not measured or has no such feature: the reason, no bar.
     * Row or column class "us" is tsdb, drawn in the primary colour. data-limit draws a memory-limit line. */

    var SVGNS = "http://www.w3.org/2000/svg";

    function el(name, attrs, text) {
        var e = document.createElementNS(SVGNS, name);
        for (var k in attrs) if (Object.prototype.hasOwnProperty.call(attrs, k)) e.setAttribute(k, attrs[k]);
        if (text != null) e.textContent = text;
        return e;
    }

    function cellText(td) {
        return td.getAttribute("data-label") || td.textContent.replace(/\s+/g, " ").trim();
    }

    function rowsOf(table) {
        var kind = table.getAttribute("data-kind") || "bars";
        var out = [];
        var trs = table.tBodies[0].rows;
        if (kind === "bars") {
            for (var i = 0; i < trs.length; i++) {
                var th = trs[i].cells[0], td = trs[i].cells[1];
                out.push({ group: null, items: [item(th.getAttribute("data-short") || th.textContent.trim(), td,
                    trs[i].classList.contains("us"))] });
            }
            return out;
        }
        var heads = table.tHead.rows[0].cells;
        for (var r = 0; r < trs.length; r++) {
            var items = [];
            for (var c = 1; c < trs[r].cells.length; c++)
                items.push(item(heads[c].textContent.trim(), trs[r].cells[c], heads[c].classList.contains("us")));
            out.push({ group: trs[r].cells[0].getAttribute("data-short") || trs[r].cells[0].textContent.trim(),
                items: items });
        }
        return out;
    }

    function item(label, td, us) {
        return {
            label: label,
            us: us,
            v: td.hasAttribute("data-v") ? parseFloat(td.getAttribute("data-v")) : null,
            text: cellText(td),
            fail: td.getAttribute("data-fail"),
            na: td.getAttribute("data-na")
        };
    }

    var measureCtx = null;

    function textWidth(s, host) {
        if (!measureCtx) {
            var c = document.createElement("canvas");
            measureCtx = c.getContext && c.getContext("2d");
            if (!measureCtx) return s.length * 7.2;
        }
        measureCtx.font = "700 13px " + window.getComputedStyle(host).fontFamily;
        return measureCtx.measureText(s).width;
    }

    function drawChart(fig) {
        var table = fig.querySelector("table.chart-data");
        var host = fig.querySelector(".chart-svg");
        if (!table || !host) return;
        var groups = rowsOf(table);
        var W = Math.max(260, Math.floor(host.clientWidth || 520));
        /* the label column is as wide as the longest label, within limits; narrower than that, labels go above */
        var longest = 0;
        for (var m = 0; m < groups.length; m++)
            for (var q = 0; q < groups[m].items.length; q++)
                longest = Math.max(longest, textWidth(groups[m].items[q].label, host));
        var narrow = W < 460 || longest > W * 0.42;
        var labelW = narrow ? 0 : Math.ceil(longest) + 4;
        var valW = narrow ? 78 : 92;
        var barX = narrow ? 0 : labelW + 10;
        var barMax = W - barX - valW;
        var rowH = narrow ? 42 : 30;
        var groupH = 26;
        var limit = table.hasAttribute("data-limit") ? parseFloat(table.getAttribute("data-limit")) : null;
        var globalMax = 0;
        for (var g = 0; g < groups.length; g++)
            for (var i = 0; i < groups[g].items.length; i++)
                if (groups[g].items[i].v != null) globalMax = Math.max(globalMax, groups[g].items[i].v);
        if (limit) globalMax = Math.max(globalMax, limit);

        var y = 0, n = 0;
        var svg = el("svg", { role: "img", width: W });
        var aria = [];
        var body = el("g", {});
        for (g = 0; g < groups.length; g++) {
            var grp = groups[g];
            var max = globalMax;
            if (grp.group != null) {
                max = 0;
                for (i = 0; i < grp.items.length; i++) if (grp.items[i].v != null) max = Math.max(max, grp.items[i].v);
                body.appendChild(el("text", { x: 0, y: y + 18, "class": "grp" }, grp.group));
                y += groupH;
                aria.push(grp.group + ":");
            }
            for (i = 0; i < grp.items.length; i++) {
                var it = grp.items[i];
                var row = el("g", { "class": it.us ? "us" : "" });
                var d = (n++ * 0.06).toFixed(2) + "s";
                var by = narrow ? y + 19 : y + 7;
                var bh = narrow ? 15 : 16;
                row.appendChild(el("text", { x: 0, y: narrow ? y + 14 : y + 20, "class": "lab" }, it.label));
                if (it.fail || it.na) {
                    var msg = it.fail || it.na;
                    if (it.fail) {
                        row.appendChild(el("rect", { x: barX, y: by - 2, width: W - barX, height: bh + 4, rx: 4,
                            "class": "fail-box", style: "--d:" + d }));
                    }
                    row.appendChild(el("text", { x: barX + (it.fail ? 8 : 0), y: by + bh - 3,
                        "class": it.fail ? "fail-text" : "na-text", style: "--d:" + d }, msg));
                    aria.push(it.label + " " + msg);
                } else {
                    var w = Math.max(2, (it.v / (max || 1)) * barMax);
                    row.appendChild(el("rect", { x: barX, y: by, width: w.toFixed(1), height: bh, rx: 3,
                        "class": "bar", style: "--d:" + d }));
                    row.appendChild(el("text", { x: (barX + w + 6).toFixed(1), y: by + bh - 3, "class": "val",
                        style: "--d:" + d }, it.text));
                    aria.push(it.label + " " + it.text);
                }
                body.appendChild(row);
                y += rowH;
            }
            if (grp.group != null) y += 8;
        }
        if (limit) {
            var lx = barX + (limit / globalMax) * barMax;
            body.insertBefore(el("line", { x1: lx, x2: lx, y1: -4, y2: y, "class": "limit" }), body.firstChild);
            var lt = table.getAttribute("data-limit-label") || "limit";
            body.appendChild(el("text", { x: lx - 4, y: y + 14, "class": "limit-text", "text-anchor": "end" }, lt));
            y += 18;
            aria.push(lt);
        }
        svg.appendChild(body);
        svg.setAttribute("viewBox", "0 -4 " + W + " " + (y + 8));
        svg.setAttribute("height", y + 8);
        var title = fig.querySelector("figcaption h3");
        svg.setAttribute("aria-label", (title ? title.textContent.trim() + ". " : "") + aria.join("; "));
        host.textContent = "";
        host.appendChild(svg);
        /* the table is written open, so that it is the chart without the script; with the chart drawn it folds */
        var det = fig.querySelector("details");
        if (det && !fig.hasAttribute("data-drawn")) det.open = false;
        fig.setAttribute("data-drawn", "");
    }

    function initCharts() {
        var figs = document.querySelectorAll("figure.chart");
        if (!figs.length) return;
        for (var i = 0; i < figs.length; i++) drawChart(figs[i]);
        if (reduceMotion || !("IntersectionObserver" in window)) {
            for (i = 0; i < figs.length; i++) figs[i].classList.add("in");
        } else {
            var io = new IntersectionObserver(function (entries) {
                for (var j = 0; j < entries.length; j++) {
                    if (entries[j].isIntersecting) {
                        entries[j].target.classList.add("in");
                        io.unobserve(entries[j].target);
                    }
                }
            }, { threshold: 0.3 });
            for (i = 0; i < figs.length; i++) io.observe(figs[i]);
        }
        var lastW = window.innerWidth, timer = 0;
        window.addEventListener("resize", function () {
            if (window.innerWidth === lastW) return;
            lastW = window.innerWidth;
            clearTimeout(timer);
            timer = setTimeout(function () {
                for (var k = 0; k < figs.length; k++) drawChart(figs[k]);
            }, 150);
        });
    }

    function start() {
        initTheme();
        initBar();
        initDrawer();
        initTabs();
        initCopy();
        initCharts();
    }

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
    else start();
})();
