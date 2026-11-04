/* The hero's car: a generic modern open-wheel racing car (no livery, no team's shapes) as a 3D wireframe of about
 * 5,000 edges in metres, projected with a simple perspective camera from slightly above into SVG paths. The body is
 * a surface mesh: lofted cross-section loops along the car with longitudinal lines through them (monocoque and
 * nose, sidepods with their undercut, engine cover); the wings are lofted airfoils, the halo a tube, the tyres tori.
 * Edges go into three paths by depth (near, middle, far), drawn thinner and dimmer the further back they are. The
 * view is fixed; the speed effect (streaks, a motion trail, the floor streaming past) is CSS animation on the SVG,
 * so nothing runs in JavaScript on the page.
 *
 *     node tools/site-car.js          # prints the hero's <svg>
 *     node tools/site-car.js --stats  # edges, segments drawn and bytes
 *
 * site/index.html holds that output between <!-- site-car --> markers; make check-site runs this and fails when
 * they differ (tests/check_site.py --fix writes it again). The paths are compact: relative moves rounded to a tenth
 * of a pixel, polylines kept as one run, and segments shorter than half a pixel merged into the next.
 *
 * Axes: x along the car (nose at -x), y up, z across. */
(function () {
    "use strict";

    var lines = []; /* polylines: arrays of [x, y, z] */

    function poly(pts, closed) {
        if (pts.length < 2) return;
        lines.push(closed ? pts.concat([pts[0]]) : pts);
    }

    function line(a, b) {
        lines.push([a, b]);
    }

    function both(fn) {
        fn(1);
        fn(-1);
    }

    function lerp(a, b, t) {
        return a + (b - a) * t;
    }

    /* piecewise-linear interpolation of keyframes [x, v1, v2, ...] at x */
    function keyed(keys, x) {
        if (x <= keys[0][0]) return keys[0].slice(1);
        for (var i = 0; i + 1 < keys.length; i++) {
            var a = keys[i], b = keys[i + 1];
            if (x <= b[0]) {
                var t = (x - a[0]) / (b[0] - a[0]);
                /* smoothstep, so the surface has no visible kinks at the keyframes */
                t = t * t * (3 - 2 * t);
                return a.slice(1).map(function (v, k) { return lerp(v, b[k + 1], t); });
            }
        }
        return keys[keys.length - 1].slice(1);
    }

    /* A rounded section (a superellipse): centre (cz, cy), half sizes hz, hy, roundness n (2 an ellipse, larger
     * squarer), m points; under (0..1) pulls the lower outer side inwards: the sidepods' undercut. */
    function section(x, cz, cy, hz, hy, n, m, under, side) {
        var pts = [];
        for (var i = 0; i < m; i++) {
            var a = (i / m) * Math.PI * 2;
            var c = Math.cos(a), s = Math.sin(a);
            var pz = Math.sign(c) * Math.pow(Math.abs(c), 2 / n), py = Math.sign(s) * Math.pow(Math.abs(s), 2 / n);
            var z = hz * pz;
            if (under && py < 0 && pz * (side || 1) > 0) z *= 1 - under * -py;
            pts.push([x, cy + hy * py, cz + z]);
        }
        return pts;
    }

    /* a lofted surface: section loops at each x (closed rings) and lines along the car through every k-th point;
     * skip(x, i) leaves a point out of both (the cockpit opening) */
    function loft(xs, make, every, skip) {
        var rings = xs.map(make);
        rings.forEach(function (r, ri) {
            var run = [];
            for (var i = 0; i <= r.length; i++) {
                var k = i % r.length;
                if (skip && skip(xs[ri], k, r.length)) {
                    poly(run);
                    run = [];
                } else run.push(r[k]);
            }
            poly(run);
        });
        var m = rings[0].length;
        for (var k = 0; k < m; k += every || 1) {
            var run2 = [];
            for (var j = 0; j < rings.length; j++) {
                if (skip && skip(xs[j], k, m)) {
                    poly(run2);
                    run2 = [];
                } else run2.push(rings[j][k]);
            }
            poly(run2);
        }
    }

    function range(a, b, n) {
        var out = [];
        for (var i = 0; i < n; i++) out.push(lerp(a, b, i / (n - 1)));
        return out;
    }

    /* an airfoil loop in the x-y plane: leading edge at x0, chord c, thickness t, angle (nose up positive) */
    function airfoil(x0, y0, z, c, t, angle, m, camb) {
        var pts = [];
        var ca = Math.cos(angle), sa = Math.sin(angle);
        for (var i = 0; i < m; i++) {
            var u = i / m * 2, top = u <= 1, p = top ? u : 2 - u; /* 0 at the leading edge, 1 at the trailing */
            var th = 5 * t * (0.2969 * Math.sqrt(p) - 0.126 * p - 0.3516 * p * p + 0.2843 * p * p * p - 0.1036 * p * p * p * p);
            var camber = (camb || 0.06) * Math.sin(Math.PI * p);
            var px = p * c, py = (camber + (top ? th : -th)) * c;
            pts.push([x0 + px * ca + py * sa, y0 - px * sa + py * ca, z]);
        }
        return pts;
    }

    /* ---- monocoque and nose cone: a rounded section from the nose tip to the gearbox, open at the cockpit ---- */
    var MONO = [
        /* x, half width, bottom, top, roundness */
        [-2.86, 0.035, 0.26, 0.32, 2.2],
        [-2.5, 0.075, 0.22, 0.38, 2.4],
        [-2.0, 0.13, 0.17, 0.46, 2.6],
        [-1.45, 0.21, 0.1, 0.54, 2.8],
        [-0.95, 0.29, 0.07, 0.63, 3.0],
        [-0.55, 0.36, 0.06, 0.68, 3.2],
        [0.0, 0.4, 0.06, 0.7, 3.4],
        [0.45, 0.39, 0.06, 0.72, 3.4],
        [1.0, 0.31, 0.07, 0.68, 3.0],
        [1.6, 0.22, 0.1, 0.58, 2.8],
        [2.15, 0.14, 0.14, 0.46, 2.6],
        [2.42, 0.1, 0.17, 0.4, 2.4]
    ];
    loft(range(-2.86, 2.42, 30), function (x) {
        var k = keyed(MONO, x);
        return section(x, 0, (k[1] + k[2]) / 2, k[0], (k[2] - k[1]) / 2, k[3], 20);
    }, 1, function (x, i, m) {
        /* the cockpit opening: the upper points between the front of the halo and the roll hoop */
        var a = (i / m) * Math.PI * 2;
        return x > -0.5 && x < 0.32 && Math.sin(a) > 0.55 && Math.abs(Math.cos(a)) < 0.75;
    });

    /* cockpit rim */
    var rim = [];
    for (var i = 0; i <= 28; i++) {
        var a = (i / 28) * Math.PI * 2;
        rim.push([-0.09 + 0.41 * Math.cos(a), 0.71 + 0.01 * Math.cos(a), 0.27 * Math.sin(a)]);
    }
    poly(rim);

    /* ---- engine cover with the airbox above the driver's head, and the shark fin ---- */
    var COVER = [
        [0.32, 0.15, 0.66, 1.1],
        [0.6, 0.2, 0.66, 1.08],
        [1.1, 0.19, 0.64, 0.9],
        [1.7, 0.14, 0.56, 0.72],
        [2.3, 0.08, 0.44, 0.52]
    ];
    loft(range(0.32, 2.3, 16), function (x) {
        var k = keyed(COVER, x);
        return section(x, 0, (k[1] + k[2]) / 2, k[0], (k[2] - k[1]) / 2, 2.6, 14);
    }, 1);
    /* the airbox inlet */
    poly(section(0.31, 0, 0.92, 0.09, 0.06, 2.2, 12), true);
    /* shark fin: its top edge and ribs down to the cover */
    var fin = [];
    range(0.75, 2.45, 14).forEach(function (x, k) {
        var top = lerp(1.0, 0.86, k / 13) - (x > 2.2 ? (x - 2.2) * 1.2 : 0);
        var base = keyed(COVER, Math.min(x, 2.3))[2];
        fin.push([x, top, 0]);
        line([x, top, 0], [x, base, 0]);
    });
    poly(fin);

    /* ---- sidepods, with the inlet at the front and the undercut below, narrowing to the "coke bottle" ---- */
    var POD = [
        /* x, inner z, width, bottom, top */
        [-0.55, 0.36, 0.4, 0.2, 0.58],
        [-0.25, 0.36, 0.43, 0.15, 0.6],
        [0.35, 0.36, 0.42, 0.13, 0.58],
        [0.95, 0.31, 0.32, 0.12, 0.5],
        [1.45, 0.24, 0.2, 0.12, 0.38],
        [1.9, 0.17, 0.09, 0.14, 0.28]
    ];
    both(function (s) {
        loft(range(-0.55, 1.9, 12), function (x) {
            var k = keyed(POD, x);
            return section(x, s * (k[0] + k[1] / 2), (k[2] + k[3]) / 2, k[1] / 2, (k[3] - k[2]) / 2, 3.2, 16, 0.45, s);
        }, 1);
        /* the inlet's lip */
        poly(section(-0.57, s * (0.36 + 0.2), 0.42, 0.17, 0.12, 4, 14), true);
    });

    /* ---- floor, edge wing, fences under the front, diffuser with strakes ---- */
    var floorEdge = [[-1.2, 0.04, 0.22], [-0.95, 0.04, 0.55], [-0.7, 0.04, 0.84], [0.6, 0.04, 0.86], [1.3, 0.04, 0.84],
        [1.6, 0.04, 0.7], [1.95, 0.04, 0.56]];
    both(function (s) {
        var e = floorEdge.map(function (p) { return [p[0], p[1], p[2] * s]; });
        poly(e);
        poly(floorEdge.slice(2, 6).map(function (p) { return [p[0], 0.075, (p[2] - 0.02) * s]; })); /* edge wing */
        floorEdge.slice(2, 6).forEach(function (p) { line([p[0], 0.04, p[2] * s], [p[0], 0.075, (p[2] - 0.02) * s]); });
        [0.28, 0.4, 0.52, 0.64].forEach(function (z) { /* fences */
            poly([[-1.05, 0.04, z * s], [-0.75, 0.12, z * s], [-0.3, 0.13, (z + 0.04) * s], [0.1, 0.05, (z + 0.06) * s]]);
        });
    });
    line([-1.2, 0.04, 0.22], [-1.2, 0.04, -0.22]);
    /* diffuser: its roof rising to the back, the strakes below it */
    var diff = range(-0.55, 0.55, 9);
    diff.forEach(function (z) {
        poly([[1.8, 0.04, z], [2.1, 0.12, z], [2.4, 0.24, z], [2.58, 0.32, z]]);
        if (Math.abs(z) > 0.01 && Math.abs(z) < 0.5) poly([[1.95, 0.04, z], [2.3, 0.04, z], [2.58, 0.12, z]]);
    });
    [1.8, 2.1, 2.4, 2.58].forEach(function (x, k) {
        var y = [0.04, 0.12, 0.24, 0.32][k];
        poly([[x, y, -0.55], [x, y, 0.55]]);
    });

    /* ---- front wing: four elements, rising and curling up towards the endplates, nose pylons ---- */
    var FW = [
        /* leading edge x, height, chord, angle */
        [-3.02, 0.07, 0.24, 0.04],
        [-2.83, 0.12, 0.2, 0.22],
        [-2.69, 0.17, 0.15, 0.42],
        [-2.59, 0.22, 0.12, 0.62]
    ];
    FW.forEach(function (el, n) {
        var zs = range(-0.95, 0.95, 9);
        loft(zs, function (z) {
            var t = Math.max(0, (Math.abs(z) - 0.28) / 0.67); /* 0 at the neutral middle, 1 at the endplate */
            var lift = n === 0 ? 0.01 * t : (0.015 + 0.02 * n) * t * t;
            var x0 = el[0] + 0.06 * t * t;
            return airfoil(x0, el[1] + lift, z, el[2] * (1 - 0.2 * t), 0.1, el[3] + 0.1 * t, 8)
                .map(function (p) { return [p[0], p[1], p[2]]; });
        }, 1);
    });
    both(function (s) {
        var z = 0.97 * s;
        poly([[-3.12, 0.02, z], [-2.45, 0.03, z], [-2.38, 0.22, z + 0.02 * s], [-2.46, 0.5, z + 0.06 * s],
            [-2.8, 0.46, z + 0.05 * s], [-3.06, 0.24, z + 0.02 * s]],
            true);
        poly([[-2.9, 0.06, z], [-2.55, 0.36, z]]);
        poly([[-3.0, 0.18, z], [-2.62, 0.4, z]]);
        /* nose pylons: thin quads from the nose down to the wing */
        poly([[-2.45, 0.2, 0.05 * s], [-2.58, 0.1, 0.12 * s], [-2.7, 0.1, 0.12 * s], [-2.58, 0.21, 0.05 * s]], true);
    });

    /* ---- rear wing: mainplane, DRS flap, endplates, swan-neck mounts, beam wing ----
     * A high-downforce wing nearly as wide as the rear wheels' outer edges (the wheels reach about 0.99 m out):
     * deeply cambered elements, twisted to a steeper angle and swept up towards the endplates, a spoon-shaped dip
     * in the middle. */
    var RWZ = 0.9, rearWingFrom = lines.length;
    var RW = [
        /* leading edge x, height, chord, angle, camber */
        [2.46, 0.86, 0.38, 0.14, 0.1],
        [2.68, 1.0, 0.27, 0.5, 0.09]
    ];
    RW.forEach(function (el) {
        loft(range(-RWZ, RWZ, 17), function (z) {
            var u = Math.abs(z) / RWZ;
            var dip = 0.015 * Math.cos(u * Math.PI / 2); /* a slight spoon */
            var tip = 0.01 * u * u; /* the tips meet the endplates */
            return airfoil(el[0] - 0.02 * u, el[1] - dip + tip, z, el[2], 0.12, el[3] + 0.04 * u, 10, el[4]);
        }, 1);
    });
    both(function (s) {
        var z = (RWZ + 0.02) * s;
        /* endplate: a tall plate, with louvres along the bottom, slots at the top trailing corner and a footplate */
        poly([[2.32, 0.26, z], [3.02, 0.28, z], [3.04, 1.14, z], [3.0, 1.22, z], [2.92, 1.26, z], [2.46, 1.26, z],
            [2.36, 1.08, z], [2.38, 0.62, z]], true);
        [0.38, 0.46, 0.54, 0.62, 0.7].forEach(function (y) { line([2.44, y, z], [2.96, y + 0.04, z]); }); /* louvres */
        [2.78, 2.86, 2.94].forEach(function (x) { line([x, 1.02, z], [x + 0.04, 1.2, z]); }); /* slots */
        poly([[2.3, 0.26, z], [2.3, 0.26, z - 0.1 * s], [3.0, 0.3, z - 0.1 * s], [3.0, 0.3, z]]); /* footplate */
        line([3.04, 1.2, z], [3.04, 1.2, z - 0.06 * s]); /* the endplate's top lip */
        /* swan neck: from the mainplane's top down and forward to the gearbox, as a narrow strip */
        var neck = [];
        for (var k = 0; k <= 10; k++) {
            var t = k / 10;
            neck.push([lerp(2.62, 2.28, t) + 0.12 * Math.sin(Math.PI * t), lerp(0.93, 0.44, t), 0.035 * s]);
        }
        poly(neck);
        poly(neck.map(function (p) { return [p[0] + 0.06, p[1] - 0.02, p[2]]; }));
        line(neck[0], [neck[0][0] + 0.06, neck[0][1] - 0.02, neck[0][2]]);
    });
    /* beam wing: two elements low down, across the diffuser to the endplates */
    loft(range(-RWZ, RWZ, 13), function (z) { return airfoil(2.44, 0.42, z, 0.22, 0.1, 0.25, 8, 0.08); }, 1);
    loft(range(-RWZ, RWZ, 13), function (z) { return airfoil(2.64, 0.48, z, 0.16, 0.1, 0.45, 8, 0.08); }, 1);

    /* the rear wing sits at the back of the view, but it is what reads as downforce: draw it one layer nearer */
    for (var rw = rearWingFrom; rw < lines.length; rw++) lines[rw].boost = 1;

    /* ---- halo: a tube from the centre pillar round the driver's head to the legs behind ---- */
    function tube(path, r, m) {
        var rings = [];
        for (var i = 0; i < path.length; i++) {
            var p = path[i], q = path[Math.min(i + 1, path.length - 1)], o = path[Math.max(i - 1, 0)];
            var t = [q[0] - o[0], q[1] - o[1], q[2] - o[2]];
            var tl = Math.hypot(t[0], t[1], t[2]) || 1;
            t = [t[0] / tl, t[1] / tl, t[2] / tl];
            var up = Math.abs(t[1]) > 0.9 ? [1, 0, 0] : [0, 1, 0];
            var u = [t[1] * up[2] - t[2] * up[1], t[2] * up[0] - t[0] * up[2], t[0] * up[1] - t[1] * up[0]];
            var ul = Math.hypot(u[0], u[1], u[2]);
            u = [u[0] / ul, u[1] / ul, u[2] / ul];
            var v = [t[1] * u[2] - t[2] * u[1], t[2] * u[0] - t[0] * u[2], t[0] * u[1] - t[1] * u[0]];
            var ring = [];
            for (var k = 0; k < m; k++) {
                var a = (k / m) * Math.PI * 2, c = Math.cos(a) * r, s = Math.sin(a) * r;
                ring.push([p[0] + u[0] * c + v[0] * s, p[1] + u[1] * c + v[1] * s, p[2] + u[2] * c + v[2] * s]);
            }
            rings.push(ring);
            poly(ring, true);
        }
        for (var k2 = 0; k2 < m; k2++) poly(rings.map(function (rg) { return rg[k2]; }));
    }
    var pillar = [];
    for (var j = 0; j <= 5; j++) pillar.push([lerp(-0.6, -0.5, j / 5), lerp(0.66, 0.87, j / 5), 0]);
    tube(pillar, 0.025, 8);
    both(function (s) {
        var hoop = [];
        for (var k = 0; k <= 14; k++) {
            var th = Math.PI - (k / 14) * Math.PI * 0.62;
            hoop.push([-0.08 + 0.42 * Math.cos(th), 0.87 - 0.03 * (k / 14), 0.3 * Math.sin(th) * s]);
        }
        for (var k3 = 1; k3 <= 5; k3++) {
            var e = hoop[14];
            hoop.push([e[0] + 0.03 * k3, e[1] - 0.045 * k3, e[2] + 0.004 * k3 * s]);
        }
        tube(hoop, 0.025, 8);
        /* mirror: a swept, pointed pod (sharp at the front, the glass facing back), set on a small cambered wing
         * from the chassis shoulder, with a winglet at its outer end */
        var MIR = [[-0.94, 0.004, 0.003], [-0.88, 0.035, 0.018], [-0.8, 0.07, 0.032], [-0.72, 0.085, 0.04],
            [-0.66, 0.088, 0.042]];
        loft(range(-0.94, -0.66, 8), function (x) {
            var k = keyed(MIR, x), back = (x + 0.94) / 0.28;
            return section(x, (0.47 + 0.05 * back) * s, 0.78 + 0.015 * back, k[0], k[1], 3.6, 10);
        }, 1);
        poly(section(-0.665, 0.52 * s, 0.795, 0.068, 0.03, 3, 10), true); /* the glass */
        /* the mounting wing: a thin cambered blade, swept back as it rises to the pod */
        loft(range(0, 1, 6), function (t) {
            var z = lerp(0.26, 0.47, t) * s;
            return airfoil(lerp(-0.9, -0.79, t), lerp(0.68, 0.755, t), z, lerp(0.1, 0.07, t), 0.08, 0.05, 8, 0.07);
        }, 1);
        /* a second, thinner strut behind it */
        poly([[-0.7, 0.65, 0.36 * s], [-0.68, 0.7, 0.42 * s], [-0.69, 0.76, 0.5 * s]]);
        /* winglet: a small swept fin under the pod's outer end */
        poly([[-0.86, 0.77, 0.6 * s], [-0.72, 0.775, 0.62 * s], [-0.7, 0.72, 0.64 * s], [-0.8, 0.71, 0.63 * s]], true);
    });

    /* ---- wheels: tyres as tori, rims with spokes, brake ducts, wheel covers ---- */
    function wheel(cx, cy, cz, R, width, s) {
        var rimR = 0.23, tube = (R - rimR) / 2, mid = rimR + tube, hw = width / 2;
        var major = 22, minor = 8, k;
        var grid = [];
        for (var i = 0; i < major; i++) {
            var a = (i / major) * Math.PI * 2, ca = Math.cos(a), sa = Math.sin(a);
            var ring = [];
            for (k = 0; k < minor; k++) {
                var b = (k / minor) * Math.PI * 2;
                var rr = mid + tube * Math.cos(b) * 1.05, zz = hw * Math.sin(b) * 0.98;
                ring.push([cx + ca * rr, cy + sa * rr, cz + zz]);
            }
            grid.push(ring);
            poly(ring, true);
        }
        for (k = 0; k < minor; k++) poly(grid.map(function (r) { return r[k]; }), true);
        /* rim face, spokes and the hub nut on the outer side; the wheel cover's rim on the inner side */
        var zo = cz + s * hw * 0.7, zi = cz - s * hw * 0.7, face = [], hub = [], cover = [];
        for (i = 0; i < 24; i++) {
            var a2 = (i / 24) * Math.PI * 2;
            face.push([cx + Math.cos(a2) * rimR, cy + Math.sin(a2) * rimR, zo]);
            cover.push([cx + Math.cos(a2) * rimR, cy + Math.sin(a2) * rimR, zi]);
            if (i % 3 === 0) hub.push([cx + Math.cos(a2) * 0.05, cy + Math.sin(a2) * 0.05, zo + 0.02 * s]);
        }
        poly(face, true);
        poly(cover, true);
        poly(hub, true);
        for (i = 0; i < 12; i++) {
            var a3 = (i / 12) * Math.PI * 2;
            line([cx + Math.cos(a3) * 0.06, cy + Math.sin(a3) * 0.06, zo + 0.01 * s],
                [cx + Math.cos(a3 + 0.12) * rimR * 0.97, cy + Math.sin(a3 + 0.12) * rimR * 0.97, zo]);
        }
        /* brake duct: a scoop inside the wheel */
        var d = cz - s * (hw + 0.02);
        poly([[cx - 0.1, cy + 0.06, d], [cx + 0.12, cy + 0.08, d], [cx + 0.12, cy - 0.08, d], [cx - 0.1, cy - 0.06, d]],
            true);
        poly([[cx - 0.1, cy + 0.06, d], [cx - 0.18, cy + 0.05, d - 0.06 * s], [cx - 0.18, cy - 0.05, d - 0.06 * s],
            [cx - 0.1, cy - 0.06, d]]);
    }
    var FX = -1.65, RX = 1.95, FR = 0.385, RR = 0.395;

    /* suspension arms as thin quads (a narrow strip from inboard to the upright) */
    function arm(a, b, w) {
        var dx = b[0] - a[0], dz = b[2] - a[2], l = Math.hypot(dx, dz) || 1;
        var ox = -dz / l * (w || 0.012), oz = dx / l * (w || 0.012);
        poly([[a[0] + ox, a[1], a[2] + oz], [b[0] + ox, b[1], b[2] + oz], [b[0] - ox, b[1], b[2] - oz],
            [a[0] - ox, a[1], a[2] - oz]], true);
    }

    both(function (s) {
        wheel(FX, FR, 0.82 * s, FR, 0.37, s);
        wheel(RX, RR, 0.8 * s, RR, 0.47, s);
        var fu = [FX, FR + 0.12, 0.64 * s], fl = [FX, FR - 0.1, 0.65 * s];
        arm([-1.95, 0.46, 0.14 * s], fu);
        arm([-1.4, 0.48, 0.18 * s], fu);
        arm([-1.98, 0.24, 0.16 * s], fl);
        arm([-1.35, 0.22, 0.2 * s], fl);
        arm([-1.55, 0.58, 0.16 * s], [FX + 0.02, FR - 0.08, 0.6 * s], 0.008); /* pushrod */
        arm([-1.85, 0.3, 0.14 * s], [FX - 0.12, FR - 0.02, 0.62 * s], 0.006); /* track rod */
        var ru = [RX, RR + 0.14, 0.58 * s], rl = [RX, RR - 0.1, 0.6 * s];
        arm([1.7, 0.5, 0.18 * s], ru);
        arm([2.25, 0.5, 0.14 * s], ru);
        arm([1.65, 0.2, 0.22 * s], rl);
        arm([2.25, 0.2, 0.16 * s], rl);
        arm([2.0, 0.18, 0.18 * s], [RX - 0.02, RR + 0.1, 0.56 * s], 0.008); /* pullrod */
        arm([2.3, 0.3, 0.14 * s], [RX + 0.14, RR, 0.58 * s], 0.006); /* toe link */
    });

    /* ---- the camera and the paths ---- */

    var CENTER_X = 0.0, CENTER_Y = 0.5;

    /* The view: three-quarter front from the car's left, nose towards the lower right, the camera about 25 degrees
     * above the car and very close to it with a short focal length: a wide-angle look, the front wing and tyres large,
     * the rear wing far away. The camera is rolled a little (about 7 degrees). */
    var VIEW = { angle: -2.45, pitch: -0.44, dist: 4.3, f: 250, roll: -0.12 };

    function proj(x, y, z) {
        var ca = Math.cos(VIEW.angle), sa = Math.sin(VIEW.angle);
        var cp = Math.cos(VIEW.pitch), sp = Math.sin(VIEW.pitch);
        x -= CENTER_X;
        y -= CENTER_Y;
        var rx = x * ca + z * sa, rz = -x * sa + z * ca; /* turn around the vertical axis */
        var ry = y * cp - rz * sp, rz2 = y * sp + rz * cp; /* tilt: a negative pitch looks down on the car */
        var k = VIEW.f / (VIEW.dist + rz2);
        /* roll: the camera tilted a little sideways, as a hand-held wide-angle shot */
        var cr = Math.cos(VIEW.roll), sr = Math.sin(VIEW.roll);
        var px = rx * k, py = -ry * k;
        return [px * cr - py * sr, px * sr + py * cr, rz2];
    }

    function f1(v) {
        return (Math.round(v * 10) / 10).toFixed(1);
    }

    /* a number for a path, as short as it goes: tenths, no trailing ".0", no leading "0" */
    function num(t) { /* t in tenths, an integer */
        var neg = t < 0, a = Math.abs(t), w = Math.floor(a / 10), f = a % 10;
        var s = f ? (w ? w + "." + f : "." + f) : String(w);
        return neg ? "-" + s : s;
    }

    function pair(dx, dy, first) {
        var a = num(dx), b = num(dy);
        return (first || a[0] === "-" ? a : " " + a) + (b[0] === "-" ? b : " " + b);
    }

    var LAYERS = ["far", "mid", "near"];

    function layerOf(depth) {
        return depth > 0.3 ? 0 : depth > -0.35 ? 1 : 2;
    }

    /* every polyline projected and cut into runs per depth layer; returns {d: [far, mid, near], segs, box} */
    function render() {
        var out = [[], [], []], last = [null, null, null], segs = 0;
        var box = [1e9, 1e9, -1e9, -1e9];
        for (var i = 0; i < lines.length; i++) {
            var pl = lines[i].map(function (p) {
                var q = proj(p[0], p[1], p[2]);
                return [Math.round(q[0] * 10), Math.round(q[1] * 10), q[2]];
            });
            var cur = [null, null, null]; /* the end of this polyline's open run in each layer */
            for (var k = 0; k + 1 < pl.length; k++) {
                var a = pl[k], b = pl[k + 1], L = Math.min(2, layerOf((a[2] + b[2]) / 2) + (lines[i].boost || 0));
                var from = cur[L] || a;
                if (Math.abs(b[0] - from[0]) + Math.abs(b[1] - from[1]) < 5) continue; /* under half a pixel */
                if (!cur[L] || cur[L] !== from) {
                    if (!(cur[L] && cur[L][0] === a[0] && cur[L][1] === a[1])) {
                        /* a new run: relative move from the end of the layer's previous run */
                        var p0 = last[L];
                        out[L].push(p0 ? "m" + pair(a[0] - p0[0], a[1] - p0[1], true) : "M" + pair(a[0], a[1], true));
                        out[L].push("l");
                        from = a;
                    }
                }
                if (!cur[L]) {
                    /* first segment of the run already has its move; nothing else */
                }
                out[L].push(pair(b[0] - from[0], b[1] - from[1], out[L][out[L].length - 1] === "l"));
                cur[L] = b;
                last[L] = b;
                for (var L2 = 0; L2 < 3; L2++) if (L2 !== L) cur[L2] = null;
                box[0] = Math.min(box[0], a[0], b[0]);
                box[1] = Math.min(box[1], a[1], b[1]);
                box[2] = Math.max(box[2], a[0], b[0]);
                box[3] = Math.max(box[3], a[1], b[1]);
                segs++;
            }
        }
        return { d: out.map(function (o) { return o.join(""); }), segs: segs,
            box: box.map(function (v) { return v / 10; }) };
    }

    function edgeCount() {
        return lines.reduce(function (n, l) { return n + l.length - 1; }, 0);
    }

    /* The speed effect, all in SVG with CSS animations (site.css): streaks rushing from the car backwards along
     * its direction of travel, fading out (one gradient along that direction); two faint copies of the near
     * wireframe (<use>, so it is not repeated in the file) shifted backwards, the motion trail. Streak positions
     * are fixed (a small seeded generator), so the output is the same every time and the check can compare it. */
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
            [3.04, 1.2, 0.92], [3.04, 1.2, -0.92], [2.9, 0.98, 0.5], [2.9, 0.98, -0.5], /* the rear wing's tips */
            [1.2, 0.05, 0.85], [1.2, 0.05, -0.85], [0.5, 0.75, 0.45]];
        /* streaks are lines in the scene, parallel to the car's axis and running back from it, so through the lens
         * they converge towards the vanishing point behind the car */
        var streaks = [];
        function streak(p, len, i) {
            var a = proj(p[0], p[1], p[2]), e = proj(p[0] + len, p[1], p[2]);
            streaks.push([a[0], a[1], e[0], e[1], i]);
        }
        anchors.forEach(function (p, i) {
            var dz = (rnd() - 0.5) * 0.08, dy = (rnd() - 0.5) * 0.06;
            streak([p[0] + 0.05, p[1] + dy, p[2] + dz], 1.5 + rnd() * 2.5, i);
        });
        for (var j = 0; j < 10; j++) { /* free air around the car */
            streak([-2.5 + rnd() * 5, 0.1 + rnd() * 1.2, (rnd() - 0.5) * 2.4], 1 + rnd() * 2, anchors.length + j);
        }
        void px;
        void py;
        var r = render(), b = r.box;
        var minx = b[0], miny = b[1], maxx = b[2], maxy = b[3];
        streaks.forEach(function (s) {
            miny = Math.min(miny, s[1], s[3]);
            maxy = Math.max(maxy, s[1], s[3]);
        });
        var pad = 12;
        /* the box is the car's own width, centred on it, so the car (not car and streaks together) sits in the middle
         * of the banner at its full size; the streaks that trail past the box are drawn anyway (overflow: visible) */
        var vb = [Math.floor(minx - pad), Math.floor(miny - pad), Math.ceil(maxx - minx + 2 * pad),
            Math.ceil(maxy - miny + 2 * pad)];
        var cx = (nose[0] + tail[0]) / 2, cy = (nose[1] + tail[1]) / 2;
        var out = [];
        out.push('<svg class="car car3d" viewBox="' + vb.join(" ") + '" role="img" aria-label="A detailed wireframe ' +
            'model of a modern open-wheel racing car seen from slightly above, with speed lines rushing past it">');
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
        out.push('<feGaussianBlur stdDeviation="2" result="b"/>');
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
        out.push('<use class="ghost g2" href="#car-near" transform="translate(' + f1(ux * 22) + " " + f1(uy * 22) +
            ')"/>');
        out.push('<use class="ghost g1" href="#car-near" transform="translate(' + f1(ux * 11) + " " + f1(uy * 11) +
            ')"/>');
        out.push("</g>");
        out.push('<path class="far" pathLength="1" d="' + r.d[0] + '"/>');
        out.push('<path class="mid" pathLength="1" d="' + r.d[1] + '"/>');
        out.push('<path class="near" id="car-near" pathLength="1" filter="url(#car-glow)" d="' + r.d[2] + '"/>');
        out.push("</svg>");
        return out.join("\n");
    }

    if (typeof module !== "undefined" && module.exports) {
        module.exports = { render: render, svg: svg, proj: proj, VIEW: VIEW, lines: lines, edgeCount: edgeCount };
        if (require.main === module) {
            if (process.argv[2] === "--stats") {
                var s = svg(), r = render();
                process.stdout.write("edges " + edgeCount() + ", segments drawn " + r.segs + ", svg " + s.length +
                    " bytes (far " + r.d[0].length + ", mid " + r.d[1].length + ", near " + r.d[2].length + ")\n");
            } else process.stdout.write(svg() + "\n");
        }
    }
})();
