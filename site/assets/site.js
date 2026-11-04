/* tsdb site: the phone menu, the docs sidebar and its section list, language tabs, copy buttons and the benchmark
 * charts. Plain JavaScript, no libraries. Loaded in <head>; everything waits for the document. localStorage may be
 * missing or throw (private windows, blocked storage): every access is wrapped, and the page works without it. The
 * site is dark only, so there is no theme to choose. */
(function () {
    "use strict";

    var LANG_KEY = "tsdb-lang";

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

    var reduceMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    /* ---------- top menu (phones) and docs sidebar ---------- */

    /* One panel open at a time: the top menu (body.nav-open) or the docs sidebar (body.side-open). Escape and the
     * scrim close it, and focus goes back to the button that opened it. */
    var openPanel = null;

    function setPanel(name, btn, focusEl) {
        var scrim = document.querySelector(".scrim");
        var buttons = document.querySelectorAll(".menu-btn, .side-btn");
        document.body.classList.toggle("nav-open", name === "nav");
        document.body.classList.toggle("side-open", name === "side");
        for (var i = 0; i < buttons.length; i++) buttons[i].setAttribute("aria-expanded", "false");
        if (btn && name) btn.setAttribute("aria-expanded", "true");
        if (scrim) scrim.hidden = !name;
        var was = openPanel;
        openPanel = name ? { name: name, btn: btn } : null;
        if (name && focusEl) focusEl.focus();
        if (!name && was && was.btn) was.btn.focus();
    }

    function initPanels() {
        var menu = document.querySelector(".menu-btn");
        var side = document.querySelector(".side-btn");
        var scrim = document.querySelector(".scrim");
        if (menu) {
            menu.addEventListener("click", function () {
                if (openPanel && openPanel.name === "nav") setPanel(null);
                else setPanel("nav", menu, document.querySelector(".topnav a"));
            });
        }
        if (side) {
            side.addEventListener("click", function () {
                if (openPanel && openPanel.name === "side") setPanel(null);
                else setPanel("side", side, document.querySelector('.sidebar a[aria-current="page"]'));
            });
        }
        if (scrim) scrim.addEventListener("click", function () { setPanel(null); });
        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape" && openPanel) setPanel(null);
        });
        var sidebar = document.querySelector(".sidebar");
        if (sidebar) {
            sidebar.addEventListener("click", function (e) {
                if (e.target.closest("a") && openPanel) {
                    openPanel = null;
                    setPanel(null);
                }
            });
        }
    }

    /* The current page's sections, under its link in the sidebar: read from the page's own h2 headings, so the
     * menu cannot drift from the page. The section in view is highlighted as the reader scrolls. */
    function initToc() {
        var current = document.querySelector('.sidebar a[aria-current="page"]');
        var heads = document.querySelectorAll("main.doc h2[id]");
        if (!current || !heads.length) return;
        var ul = document.createElement("ul");
        ul.className = "toc";
        var links = [];
        for (var i = 0; i < heads.length; i++) {
            var li = document.createElement("li");
            var a = document.createElement("a");
            a.href = "#" + heads[i].id;
            a.textContent = heads[i].textContent;
            li.appendChild(a);
            ul.appendChild(li);
            links.push(a);
        }
        current.parentNode.appendChild(ul);
        function mark(id) {
            for (var j = 0; j < links.length; j++) {
                var on = links[j].getAttribute("href") === "#" + id;
                links[j].classList.toggle("active", on);
                if (on) links[j].setAttribute("aria-current", "location");
                else links[j].removeAttribute("aria-current");
            }
        }
        function onScroll() {
            var top = 0, id = heads[0].id;
            var limit = (window.innerHeight || 800) * 0.3;
            for (var k = 0; k < heads.length; k++) {
                top = heads[k].getBoundingClientRect().top;
                if (top - limit <= 0) id = heads[k].id;
            }
            if (window.innerHeight + window.scrollY >= document.body.scrollHeight - 4) id = heads[heads.length - 1].id;
            mark(id);
        }
        var ticking = false;
        window.addEventListener("scroll", function () {
            if (ticking) return;
            ticking = true;
            window.requestAnimationFrame(function () {
                ticking = false;
                onScroll();
            });
        }, { passive: true });
        onScroll();
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
     * Each <figure class="chart"> holds one panel of the README's benchmark charts: a <table class="chart-data">
     * whose rows tools/readme-charts.py wrote from tools/readme-charts.json, with the bar length (data-len, 0 to 1)
     * and the label already worked out there, so the site draws exactly what the README shows. The table stays on
     * the page as the text version; the script draws an SVG from it.
     *   tr.us          tsdb, in the highlight gradient
     *   data-len       the bar's share of the bar area
     *   data-none      no number (not measured, no such function): a box with the reason, never a bar
     *   data-tag       a failure next to a bar (memory: where the system failed), drawn as a red tag */

    var SVGNS = "http://www.w3.org/2000/svg";

    function el(name, attrs, text) {
        var e = document.createElementNS(SVGNS, name);
        for (var k in attrs) if (Object.prototype.hasOwnProperty.call(attrs, k)) e.setAttribute(k, attrs[k]);
        if (text != null) e.textContent = text;
        return e;
    }

    var measureCtx = null;

    function textWidth(s, host, size, weight) {
        if (!measureCtx) {
            var c = document.createElement("canvas");
            measureCtx = c.getContext && c.getContext("2d");
        }
        if (!measureCtx) return s.length * size * 0.56;
        measureCtx.font = (weight || 400) + " " + size + "px " + window.getComputedStyle(host).fontFamily;
        return measureCtx.measureText(s).width;
    }

    function rowsOf(table) {
        var out = [];
        var trs = table.tBodies[0].rows;
        for (var i = 0; i < trs.length; i++) {
            var tr = trs[i];
            var td = tr.cells[1];
            var tag = tr.getAttribute("data-tag");
            var text = td.textContent.replace(/\s+/g, " ").trim();
            if (tag && text.slice(-(tag.length + 3)) === " (" + tag + ")") text = text.slice(0, -(tag.length + 3));
            out.push({
                name: tr.cells[0].textContent.trim(),
                us: tr.classList.contains("us"),
                len: tr.hasAttribute("data-len") ? parseFloat(tr.getAttribute("data-len")) : null,
                none: tr.getAttribute("data-none"),
                tag: tag,
                text: text
            });
        }
        return out;
    }

    function drawChart(fig) {
        var table = fig.querySelector("table.chart-data");
        var host = fig.querySelector(".chart-svg");
        if (!table || !host) return;
        var rows = rowsOf(table);
        var W = Math.max(260, Math.floor(host.clientWidth || 520));
        var nameW = 0, labelW = 0;
        for (var i = 0; i < rows.length; i++) {
            var r = rows[i];
            nameW = Math.max(nameW, textWidth(r.name, host, 13, r.us ? 800 : 400));
            if (!r.none) {
                var lw = textWidth(r.text, host, 12.5, r.us ? 800 : 400) + 8;
                if (r.tag) lw += textWidth(r.tag, host, 11.5, 600) + 26;
                labelW = Math.max(labelW, lw);
            }
        }
        /* names left of the bars where they fit, above the bars on narrow screens */
        var narrow = W - nameW - 12 - labelW < 90;
        var barX = narrow ? 0 : Math.ceil(nameW) + 12;
        var barW = Math.max(40, W - barX - labelW);
        var rowH = narrow ? 44 : 30;
        var gid = "us-" + (fig.id || "chart");
        var svg = el("svg", { role: "img", width: W });
        var defs = el("defs", {});
        var lg = el("linearGradient", { id: gid, x1: "0", x2: "1", y1: "0", y2: "0" });
        lg.appendChild(el("stop", { offset: "0", style: "stop-color:var(--cyan)" }));
        lg.appendChild(el("stop", { offset: "1", style: "stop-color:var(--violet)" }));
        defs.appendChild(lg);
        svg.appendChild(defs);
        var aria = [];
        var y = 0;
        for (i = 0; i < rows.length; i++) {
            r = rows[i];
            var g = el("g", { "class": r.us ? "us" : "" });
            var d = (i * 0.06).toFixed(2) + "s";
            var by = narrow ? y + 20 : y + 7, bh = 16;
            g.appendChild(el("text", { x: 0, y: narrow ? y + 14 : y + 20, "class": "lab" }, r.name));
            if (r.none) {
                g.appendChild(el("rect", { x: barX, y: by - 1, width: W - barX, height: bh + 2, rx: 5,
                    "class": "na-box", style: "--d:" + d }));
                g.appendChild(el("text", { x: barX + 8, y: by + bh - 4, "class": "na-text", style: "--d:" + d }, r.none));
                aria.push(r.name + ": " + r.none);
            } else {
                var w = Math.max(3, r.len * barW);
                var rect = el("rect", { x: barX, y: by, width: w.toFixed(1), height: bh, rx: 4, "class": "bar",
                    style: "--d:" + d });
                if (r.us) rect.style.fill = "url(#" + gid + ")";
                g.appendChild(rect);
                var lx = barX + w + 7;
                g.appendChild(el("text", { x: lx.toFixed(1), y: by + bh - 3.5, "class": "val", style: "--d:" + d }, r.text));
                if (r.tag) {
                    var tx = lx + textWidth(r.text, host, 12.5, 400) + 10;
                    var tw = textWidth(r.tag, host, 11.5, 600) + 16;
                    g.appendChild(el("rect", { x: tx.toFixed(1), y: by - 1, width: tw.toFixed(1), height: bh + 2,
                        rx: 9, "class": "fail-box", style: "--d:" + d }));
                    g.appendChild(el("text", { x: (tx + 8).toFixed(1), y: by + bh - 4, "class": "fail-text",
                        style: "--d:" + d }, r.tag));
                }
                aria.push(r.name + ": " + r.text + (r.tag ? ", " + r.tag : ""));
            }
            svg.appendChild(g);
            y += rowH;
        }
        svg.setAttribute("viewBox", "0 -2 " + W + " " + (y + 4));
        svg.setAttribute("height", y + 4);
        var title = fig.querySelector("figcaption h3");
        var sub = fig.querySelector("figcaption p");
        svg.setAttribute("aria-label", (title ? title.textContent.trim() + " (" : "") + (sub ? sub.textContent.trim() : "") +
            "). " + aria.join("; "));
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
        initPanels();
        initToc();
        initTabs();
        initCopy();
        initCharts();
    }

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
    else start();
})();
