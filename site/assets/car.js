/* The hero's 3D wireframe of a generic modern open-wheel racing car (no livery, no team's shapes): a few hundred
 * edges in metres, projected with a simple perspective camera into two SVG paths, near and far, so the far side is
 * drawn thinner and dimmer. It turns slowly on a turntable, about 30 frames a second, and stops while it is off
 * screen or the tab is hidden. With prefers-reduced-motion it stays at a three-quarter view (STILL). index.html
 * carries a still of that view, made by running render() under node (the file exports it there), so the hero is
 * never empty without JavaScript.
 *
 * Axes: x along the car (nose at -x), y up, z across. */
(function (root) {
    "use strict";

    var edges = []; /* [x1, y1, z1, x2, y2, z2] */

    function line(a, b) {
        edges.push([a[0], a[1], a[2], b[0], b[1], b[2]]);
    }

    function poly(pts, closed) {
        for (var i = 0; i + 1 < pts.length; i++) line(pts[i], pts[i + 1]);
        if (closed) line(pts[pts.length - 1], pts[0]);
    }

    function mirror(p) {
        return [p[0], p[1], -p[2]];
    }

    /* both sides of the car */
    function both(fn) {
        fn(1);
        fn(-1);
    }

    /* a lofted body: cross sections [x, bottom, top, half width, half width at the top]; rings and long edges */
    function loft(secs) {
        var rings = secs.map(function (s) {
            var x = s[0], b = s[1], t = s[2], w = s[3], wt = s[4];
            var m = (b + t) / 2;
            return [[x, b, -w * 0.7], [x, b, w * 0.7], [x, m, w], [x, t, wt], [x, t, -wt], [x, m, -w]];
        });
        rings.forEach(function (r) { poly(r, true); });
        for (var i = 0; i + 1 < rings.length; i++)
            for (var k = 0; k < 6; k++) line(rings[i][k], rings[i + 1][k]);
    }

    function wheel(cx, cy, cz, r, width, outward) {
        var n = 18, inner = [], outer = [], rim = [];
        var zi = cz - outward * width / 2, zo = cz + outward * width / 2;
        for (var i = 0; i < n; i++) {
            var a = (i / n) * Math.PI * 2;
            var dx = Math.cos(a) * r, dy = Math.sin(a) * r;
            inner.push([cx + dx, cy + dy, zi]);
            outer.push([cx + dx, cy + dy, zo]);
            rim.push([cx + dx * 0.62, cy + dy * 0.62, zo]);
        }
        poly(inner, true);
        poly(outer, true);
        poly(rim, true);
        for (i = 0; i < n; i += 3) line(inner[i], outer[i]);
        for (i = 0; i < n; i += 3) line([cx, cy, zo], rim[i]);
    }

    /* monocoque and nose */
    loft([
        [-2.7, 0.2, 0.3, 0.05, 0.04],
        [-2.2, 0.16, 0.38, 0.11, 0.08],
        [-1.6, 0.12, 0.5, 0.2, 0.14],
        [-1.0, 0.08, 0.6, 0.3, 0.2],
        [-0.55, 0.06, 0.66, 0.38, 0.26],
        [0.35, 0.06, 0.7, 0.4, 0.24],
        [1.3, 0.08, 0.6, 0.3, 0.12],
        [2.3, 0.14, 0.45, 0.16, 0.08]
    ]);

    /* airbox above the driver and the engine cover with its fin */
    poly([[0.25, 0.7, 0.0], [0.35, 1.02, 0.0], [0.7, 1.0, 0.0], [1.4, 0.82, 0.0], [2.3, 0.62, 0.0]]);
    both(function (s) {
        poly([[0.3, 0.7, 0.22 * s], [0.42, 0.97, 0.13 * s], [0.75, 0.95, 0.13 * s], [1.4, 0.72, 0.12 * s],
            [2.3, 0.45, 0.08 * s]]);
        line([0.42, 0.97, 0.13 * s], [0.35, 1.02, 0.0]);
    });
    poly([[0.75, 1.0, 0], [2.35, 0.98, 0], [2.4, 0.62, 0]]);

    /* sidepods: an inlet at the front, tapering towards the rear wheels (the "coke bottle") */
    both(function (s) {
        var a = [[-0.45, 0.14, 0.42 * s], [-0.45, 0.14, 0.78 * s], [-0.45, 0.56, 0.78 * s], [-0.45, 0.58, 0.42 * s]];
        var b = [[0.4, 0.1, 0.42 * s], [0.4, 0.1, 0.8 * s], [0.4, 0.52, 0.8 * s], [0.4, 0.62, 0.42 * s]];
        var c = [[1.3, 0.1, 0.32 * s], [1.3, 0.1, 0.5 * s], [1.3, 0.34, 0.5 * s], [1.3, 0.5, 0.3 * s]];
        var d = [[1.95, 0.12, 0.2 * s], [1.95, 0.12, 0.26 * s], [1.95, 0.22, 0.26 * s], [1.95, 0.3, 0.18 * s]];
        [a, b, c, d].forEach(function (r) { poly(r, true); });
        [[a, b], [b, c], [c, d]].forEach(function (p) {
            for (var k = 0; k < 4; k++) line(p[0][k], p[1][k]);
        });
    });

    /* halo: a centre pillar and a hoop around the driver's head; mirrors */
    poly([[-0.55, 0.66, 0], [-0.45, 0.86, 0]]);
    both(function (s) {
        poly([[-0.45, 0.86, 0], [-0.3, 0.88, 0.16 * s], [0.0, 0.87, 0.27 * s], [0.25, 0.82, 0.29 * s],
            [0.32, 0.68, 0.3 * s]]);
        poly([[-0.75, 0.62, 0.3 * s], [-0.8, 0.74, 0.42 * s], [-0.65, 0.76, 0.5 * s], [-0.6, 0.66, 0.42 * s]], true);
    });

    /* floor, wider than the body, with the diffuser rising at the back */
    poly([[-1.15, 0.03, 0.2], [-0.6, 0.03, 0.82], [1.55, 0.03, 0.82], [2.0, 0.03, 0.55], [2.0, 0.03, -0.55],
        [1.55, 0.03, -0.82], [-0.6, 0.03, -0.82], [-1.15, 0.03, -0.2]], true);
    both(function (s) { poly([[2.0, 0.03, 0.55 * s], [2.55, 0.28, 0.55 * s], [2.55, 0.28, 0.0]]); });

    /* front wing: three elements between two endplates */
    [[0.06, -3.0, -2.62], [0.11, -2.88, -2.58], [0.17, -2.8, -2.56]].forEach(function (e) {
        var y = e[0], x1 = e[1], x2 = e[2];
        poly([[x1, y, -0.95], [x1, y, 0.95], [x2, y + 0.05, 0.95], [x2, y + 0.05, -0.95]], true);
    });
    both(function (s) {
        poly([[-3.05, 0.03, 0.97 * s], [-2.5, 0.03, 0.97 * s], [-2.48, 0.3, 0.97 * s], [-2.85, 0.28, 0.97 * s]],
            true);
        line([-2.55, 0.22, 0.12 * s], [-2.62, 0.08, 0.2 * s]); /* the nose pillars */
    });

    /* rear wing: main plane and flap between endplates, and the beam wing below */
    [[0.86, 2.48, 2.86, 0.04], [1.0, 2.62, 2.92, 0.06]].forEach(function (e) {
        var y = e[0], x1 = e[1], x2 = e[2], h = e[3];
        poly([[x1, y, -0.52], [x1, y, 0.52], [x2, y + h, 0.52], [x2, y + h, -0.52]], true);
    });
    poly([[2.45, 0.42, -0.45], [2.45, 0.42, 0.45], [2.7, 0.46, 0.45], [2.7, 0.46, -0.45]], true);
    both(function (s) {
        poly([[2.4, 0.3, 0.54 * s], [2.95, 0.36, 0.54 * s], [2.98, 1.12, 0.54 * s], [2.45, 1.1, 0.54 * s]], true);
    });
    poly([[2.3, 0.45, 0], [2.6, 0.9, 0]]); /* the pylon */

    /* wheels and suspension arms */
    var FX = -1.65, RX = 1.95, FR = 0.36, RR = 0.37;
    both(function (s) {
        wheel(FX, FR, 0.82 * s, FR, 0.3, s);
        wheel(RX, RR, 0.8 * s, RR, 0.38, s);
        var fh = [FX, FR, 0.66 * s], rh = [RX, RR, 0.6 * s];
        poly([[-1.95, 0.24, 0.16 * s], fh, [-1.35, 0.22, 0.2 * s]]);
        poly([[-1.9, 0.46, 0.14 * s], [FX, FR + 0.12, 0.66 * s], [-1.4, 0.46, 0.18 * s]]);
        line([FX, FR - 0.05, 0.64 * s], [-1.6, 0.56, 0.16 * s]); /* pushrod */
        poly([[1.65, 0.2, 0.22 * s], rh, [2.25, 0.2, 0.16 * s]]);
        poly([[1.7, 0.5, 0.18 * s], [RX, RR + 0.14, 0.6 * s], [2.25, 0.5, 0.14 * s]]);
    });

    var CENTER_X = 0.0, CENTER_Y = 0.5;

    /* project every edge for a turntable angle; returns [far path, near path] */
    function render(angle, pitch, scale) {
        var ca = Math.cos(angle), sa = Math.sin(angle);
        var cp = Math.cos(pitch), sp = Math.sin(pitch);
        var dist = 9, f = scale || 520;
        var near = [], far = [];
        function proj(x, y, z) {
            x -= CENTER_X;
            y -= CENTER_Y;
            var rx = x * ca + z * sa, rz = -x * sa + z * ca; /* turn around the vertical axis */
            var ry = y * cp - rz * sp, rz2 = y * sp + rz * cp; /* look down a little */
            var k = f / (dist + rz2);
            return [rx * k, -ry * k, rz2];
        }
        for (var i = 0; i < edges.length; i++) {
            var e = edges[i];
            var a = proj(e[0], e[1], e[2]), b = proj(e[3], e[4], e[5]);
            var seg = "M" + a[0].toFixed(1) + " " + a[1].toFixed(1) + "L" + b[0].toFixed(1) + " " + b[1].toFixed(1);
            ((a[2] + b[2]) / 2 > 0 ? far : near).push(seg);
        }
        return [far.join(""), near.join("")];
    }

    var STILL = { angle: -0.62, pitch: 0.32 };

    function start() {
        var svg = document.querySelector(".car3d");
        if (!svg) return;
        var farP = svg.querySelector(".far"), nearP = svg.querySelector(".near");
        function draw(angle) {
            var d = render(angle, STILL.pitch);
            farP.setAttribute("d", d[0]);
            nearP.setAttribute("d", d[1]);
        }
        var reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
        draw(STILL.angle);
        if (reduce || !window.requestAnimationFrame) return;
        var visible = true, last = 0, angle = STILL.angle, running = false;
        function frame(t) {
            running = false;
            if (!visible || document.hidden) return;
            if (t - last >= 33) {
                angle += last ? (t - last) * 0.00022 : 0; /* one turn in about half a minute */
                last = t;
                draw(angle);
            }
            running = true;
            window.requestAnimationFrame(frame);
        }
        function go() {
            if (!running && visible && !document.hidden) {
                running = true;
                last = 0;
                window.requestAnimationFrame(frame);
            }
        }
        if ("IntersectionObserver" in window) {
            new IntersectionObserver(function (es) {
                visible = es[0].isIntersecting;
                go();
            }).observe(svg);
        }
        document.addEventListener("visibilitychange", go);
        go();
    }

    if (typeof module !== "undefined" && module.exports) {
        module.exports = { render: render, STILL: STILL, edges: edges };
    } else if (root.document) {
        if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
        else start();
    }
})(this);
