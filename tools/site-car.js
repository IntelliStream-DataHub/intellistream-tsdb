/* The hero's car: a generic modern open-wheel racing car (no livery, no team's shapes) as a 3D wireframe of about
 * 500 edges in metres, projected with a simple perspective camera from slightly above into SVG paths: the near
 * edges glow, the far ones are thinner and dimmer. The view is fixed; the speed effect (streaks, a motion trail,
 * the floor streaming past) is CSS animation on the SVG, so nothing runs in JavaScript on the page.
 *
 *     node tools/site-car.js          # prints the hero's <svg>
 *
 * site/index.html holds that output between <!-- site-car --> markers; make check-site runs this and fails when
 * they differ (tests/check_site.py --fix writes it again).
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

    /* The view: three-quarter front, nose towards the lower left, the camera about 17 degrees above the car so the
     * halo, airbox and wings show from the top. */
    var VIEW = { angle: -0.62, pitch: -0.3, dist: 9, f: 520 };

    function proj(x, y, z) {
        var ca = Math.cos(VIEW.angle), sa = Math.sin(VIEW.angle);
        var cp = Math.cos(VIEW.pitch), sp = Math.sin(VIEW.pitch);
        x -= CENTER_X;
        y -= CENTER_Y;
        var rx = x * ca + z * sa, rz = -x * sa + z * ca; /* turn around the vertical axis */
        var ry = y * cp - rz * sp, rz2 = y * sp + rz * cp; /* tilt: a negative pitch looks down on the car */
        var k = VIEW.f / (VIEW.dist + rz2);
        return [rx * k, -ry * k, rz2];
    }

    function f1(v) {
        return (Math.round(v * 10) / 10).toFixed(1);
    }

    /* every edge projected; [far path, near path] by the depth of the edge's middle */
    function render() {
        var near = [], far = [];
        for (var i = 0; i < edges.length; i++) {
            var e = edges[i];
            var a = proj(e[0], e[1], e[2]), b = proj(e[3], e[4], e[5]);
            var seg = "M" + f1(a[0]) + " " + f1(a[1]) + "L" + f1(b[0]) + " " + f1(b[1]);
            ((a[2] + b[2]) / 2 > 0 ? far : near).push(seg);
        }
        return [far.join(""), near.join("")];
    }

    /* The speed effect, all in SVG with CSS animations (site.css): streaks rushing from the car backwards along
     * its direction of travel, fading out (one gradient along that direction); two faint copies of the near
     * wireframe shifted backwards, the motion trail. Streak positions are fixed (a small seeded generator), so the
     * output is the same every time and the check can compare it. */
    function svg() {
        var nose = proj(-3.0, 0.3, 0), tail = proj(3.0, 0.3, 0);
        var ux = tail[0] - nose[0], uy = tail[1] - nose[1];
        var ul = Math.sqrt(ux * ux + uy * uy);
        ux /= ul;
        uy /= ul;
        var px = -uy, py = ux; /* across the direction of travel */
        var seed = 7;
        function rnd() {
            seed = (seed * 1103515245 + 12345) % 2147483648;
            return seed / 2147483648;
        }
        /* streaks start on points of the car (wing tips, wheel tops, sidepods, airbox) and behind it */
        var anchors = [[-2.9, 0.2, 0.95], [-2.9, 0.2, -0.95], [-1.65, 0.72, 0.95], [-1.65, 0.72, -0.95],
            [-0.45, 0.58, 0.78], [-0.45, 0.58, -0.78], [0.4, 1.02, 0], [1.95, 0.74, 0.98], [1.95, 0.74, -0.98],
            [2.9, 1.1, 0.54], [2.9, 1.1, -0.54], [1.2, 0.05, 0.85], [1.2, 0.05, -0.85], [0.5, 0.75, 0.45]];
        var streaks = [];
        var minx = 1e9, maxx = -1e9, miny = 1e9, maxy = -1e9;
        function grow(x, y) {
            minx = Math.min(minx, x);
            maxx = Math.max(maxx, x);
            miny = Math.min(miny, y);
            maxy = Math.max(maxy, y);
        }
        anchors.forEach(function (p, i) {
            var s = proj(p[0], p[1], p[2]);
            var off = (rnd() - 0.5) * 10, len = 90 + rnd() * 150;
            var x1 = s[0] + px * off + ux * 6, y1 = s[1] + py * off + uy * 6;
            streaks.push([x1, y1, x1 + ux * len, y1 + uy * len, i]);
        });
        for (var j = 0; j < 10; j++) { /* free air around the car */
            var c = proj(-2.5 + rnd() * 5, 0.1 + rnd() * 1.2, (rnd() - 0.5) * 2.4);
            var len2 = 60 + rnd() * 120;
            streaks.push([c[0], c[1], c[0] + ux * len2, c[1] + uy * len2, anchors.length + j]);
        }
        var paths = render();
        paths.join("").replace(/[ML](-?[\d.]+) (-?[\d.]+)/g, function (m, x, y) {
            grow(+x, +y);
            return m;
        });
        streaks.forEach(function (s) {
            grow(s[0], s[1]);
            grow(s[2], s[3]);
        });
        var pad = 12;
        var vb = [Math.floor(minx - pad), Math.floor(miny - pad), Math.ceil(maxx - minx + 2 * pad),
            Math.ceil(maxy - miny + 2 * pad)];
        var cx = (nose[0] + tail[0]) / 2, cy = (nose[1] + tail[1]) / 2;
        var out = [];
        out.push('<svg class="car car3d" viewBox="' + vb.join(" ") + '" role="img" aria-label="A wireframe model ' +
            'of a modern open-wheel racing car seen from slightly above, with speed lines rushing past it">');
        out.push("<defs>");
        out.push('<linearGradient id="car-ink" gradientUnits="userSpaceOnUse" x1="' + f1(nose[0]) + '" y1="' +
            f1(nose[1]) + '" x2="' + f1(tail[0]) + '" y2="' + f1(tail[1]) + '">');
        out.push('<stop offset="0" stop-color="#36e2ff"/><stop offset="0.55" stop-color="#8f80ff"/>' +
            '<stop offset="1" stop-color="#ff5ad9"/>');
        out.push("</linearGradient>");
        out.push('<linearGradient id="streak-ink" gradientUnits="userSpaceOnUse" x1="' + f1(cx - ux * 120) + '" y1="' +
            f1(cy - uy * 120) + '" x2="' + f1(cx + ux * 330) + '" y2="' + f1(cy + uy * 330) + '">');
        out.push('<stop offset="0" stop-color="#36e2ff" stop-opacity="0.9"/>' +
            '<stop offset="0.5" stop-color="#8f80ff" stop-opacity="0.55"/>' +
            '<stop offset="1" stop-color="#ff5ad9" stop-opacity="0"/>');
        out.push("</linearGradient>");
        out.push('<filter id="car-glow" x="-10%" y="-20%" width="120%" height="140%">');
        out.push('<feGaussianBlur stdDeviation="3" result="b"/>');
        out.push('<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>');
        out.push("</filter>");
        out.push("</defs>");
        out.push('<g class="streaks">');
        streaks.forEach(function (s) {
            out.push('<path class="streak" pathLength="1" style="--i:' + s[4] + '" d="M' + f1(s[0]) + " " + f1(s[1]) +
                "L" + f1(s[2]) + " " + f1(s[3]) + '"/>');
        });
        out.push("</g>");
        out.push('<g class="trail">');
        out.push('<path class="ghost g2" transform="translate(' + f1(ux * 22) + " " + f1(uy * 22) + ')" d="' + paths[1] +
            '"/>');
        out.push('<path class="ghost g1" transform="translate(' + f1(ux * 11) + " " + f1(uy * 11) + ')" d="' + paths[1] +
            '"/>');
        out.push("</g>");
        out.push('<path class="far" pathLength="1" d="' + paths[0] + '"/>');
        out.push('<path class="near" pathLength="1" filter="url(#car-glow)" d="' + paths[1] + '"/>');
        out.push("</svg>");
        return out.join("\n");
    }

    if (typeof module !== "undefined" && module.exports) {
        module.exports = { render: render, svg: svg, VIEW: VIEW, edges: edges };
        if (require.main === module) process.stdout.write(svg() + "\n");
    }
})(this);
