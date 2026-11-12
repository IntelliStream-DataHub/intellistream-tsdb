#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Runs .github/scripts/release-publish.sh against a local mock of the GitHub REST API and checks every request.

    python3 -I tests/test_release_publish.py        (make test runs it; needs curl and jq, else it skips)

The mock (MockGitHub) keeps releases and assets in memory, answers the calls the script makes the way GitHub
documents them (releases/tags/TAG never returns a draft; uploads reply 422 for a name that exists), checks the
token and the API version header, and records each request. It listens on 127.0.0.1, on a port the kernel picks,
inside this process. The real GitHub API is never called.
"""
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

TOP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# RELEASE_PUBLISH_SCRIPT: another copy of the script (to see that a broken one fails this test)
SCRIPT = os.environ.get("RELEASE_PUBLISH_SCRIPT") or os.path.join(TOP, ".github", "scripts", "release-publish.sh")
REPO = "acme/tsdb"
TOKEN = "ghs_mocktoken0123456789"
COMMIT = "9496076dcd01051786432dcb044920ea896eeeeb"


class MockGitHub:
    def __init__(self):
        self.lock = threading.Lock()
        self.releases = []
        self.next_id = 100
        self.log = []
        self.fail = {}  # (method, path regex) -> status, once
        self.evil_upload = False
        self.immutable = False
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                mock.handle(self, "GET")

            def do_POST(self):
                mock.handle(self, "POST")

            def do_PATCH(self):
                mock.handle(self, "PATCH")

            def do_DELETE(self):
                mock.handle(self, "DELETE")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    def new_id(self):
        self.next_id += 1
        return self.next_id

    def add_release(self, tag, draft, prerelease=False, assets=(), body="old text"):
        rel = {"id": self.new_id(), "tag_name": tag, "name": "tsdb " + tag[1:], "body": body, "draft": draft,
               "prerelease": prerelease, "make_latest": None, "assets": []}
        for name, data in assets:
            rel["assets"].append(self.asset(name, data))
        self.releases.append(rel)
        return rel

    def asset(self, name, data):
        return {"id": self.new_id(), "name": name, "size": len(data), "state": "uploaded",
                "digest": "sha256:" + hashlib.sha256(data).hexdigest(), "data": data}

    def view(self, rel):
        out = {k: v for k, v in rel.items() if k != "assets"}
        host = "http://evil.invalid" if self.evil_upload else self.url
        out["upload_url"] = "%s/repos/%s/releases/%d/assets{?name,label}" % (host, REPO, rel["id"])
        out["html_url"] = "https://github.invalid/%s/releases/tag/%s" % (REPO, rel["tag_name"])
        return out

    def handle(self, h, method):
        parts = urlsplit(h.path)
        n = int(h.headers.get("Content-Length") or 0)
        data = h.rfile.read(n) if n else b""
        ctype = h.headers.get("Content-Type", "")
        entry = {"method": method, "path": parts.path, "query": parts.query, "ctype": ctype,
                 "auth": h.headers.get("Authorization"), "accept": h.headers.get("Accept"),
                 "apiver": h.headers.get("X-GitHub-Api-Version")}
        if ctype == "application/json":
            entry["json"] = json.loads(data)
        else:
            entry["len"] = len(data)
            entry["sha256"] = hashlib.sha256(data).hexdigest()
        with self.lock:
            self.log.append(entry)
            status, body = self.route(method, parts.path, parse_qs(parts.query), data, entry)
        payload = b"" if body is None else json.dumps(body).encode()
        h.send_response(status)
        if body is not None:
            h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(payload)))
        h.end_headers()
        h.wfile.write(payload)

    def find(self, rid):
        for r in self.releases:
            if r["id"] == rid:
                return r
        return None

    def route(self, method, path, query, data, entry):
        if entry["auth"] != "Bearer " + TOKEN or entry["apiver"] != "2022-11-28":
            return 401, {"message": "Bad credentials"}
        for (m, rx), st in list(self.fail.items()):
            if m == method and re.fullmatch(rx, path):
                del self.fail[(m, rx)]
                return st, {"message": "injected %d" % st}
        base = "/repos/" + REPO
        m = re.fullmatch(base + r"/releases/tags/(.+)", path)
        if method == "GET" and m:
            for r in self.releases:
                if r["tag_name"] == m.group(1) and not r["draft"]:
                    return 200, self.view(r)
            return 404, {"message": "Not Found"}
        if method == "GET" and path == base + "/releases":
            return 200, [self.view(r) for r in reversed(self.releases)]
        if method == "POST" and path == base + "/releases":
            j = entry["json"]
            rel = self.add_release(j["tag_name"], j.get("draft", False), j.get("prerelease", False), body=j["body"])
            rel["name"] = j["name"]
            return 201, self.view(rel)
        m = re.fullmatch(base + r"/releases/(\d+)", path)
        if method == "PATCH" and m:
            rel = self.find(int(m.group(1)))
            if rel is None:
                return 404, {"message": "Not Found"}
            for k, v in entry["json"].items():
                rel[k] = v
            return 200, self.view(rel)
        m = re.fullmatch(base + r"/releases/(\d+)/assets", path)
        if method == "GET" and m:
            rel = self.find(int(m.group(1)))
            if rel is None:
                return 404, {"message": "Not Found"}
            return 200, [{k: v for k, v in a.items() if k != "data"} for a in rel["assets"]]
        if method == "POST" and m:  # the upload host, which this mock also is
            rel = self.find(int(m.group(1)))
            name = query.get("name", [""])[0]
            if rel is None:
                return 404, {"message": "Not Found"}
            if self.immutable and not rel["draft"]:
                return 422, {"message": "Cannot upload assets to an immutable release."}
            if any(a["name"] == name for a in rel["assets"]):
                return 422, {"message": "Validation Failed", "errors": [{"code": "already_exists"}]}
            a = self.asset(name, data)
            rel["assets"].append(a)
            return 201, {k: v for k, v in a.items() if k != "data"}
        m = re.fullmatch(base + r"/releases/assets/(\d+)", path)
        if method == "PATCH" and m:  # rename; GitHub refuses a name the release already has
            aid = int(m.group(1))
            for r in self.releases:
                for a in r["assets"]:
                    if a["id"] == aid:
                        if self.immutable and not r["draft"]:
                            return 422, {"message": "Cannot update assets of an immutable release."}
                        name = entry["json"].get("name", a["name"])
                        if any(b["name"] == name and b is not a for b in r["assets"]):
                            return 422, {"message": "Validation Failed", "errors": [{"code": "already_exists"}]}
                        a["name"] = name
                        return 200, {k: v for k, v in a.items() if k != "data"}
            return 404, {"message": "Not Found"}
        if method == "DELETE" and m:
            aid = int(m.group(1))
            for r in self.releases:
                for a in r["assets"]:
                    if a["id"] == aid:
                        if self.immutable and not r["draft"]:
                            return 422, {"message": "Cannot delete assets of an immutable release."}
                        r["assets"].remove(a)
                        return 204, None
            return 404, {"message": "Not Found"}
        return 404, {"message": "no route %s %s" % (method, path)}


failures = 0


def check(cond, what):
    global failures
    if not cond:
        failures += 1
        print("FAIL: " + what)


def run(mock, tag, notes, assets, env_extra=None):
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "GITHUB_TOKEN": TOKEN, "GITHUB_REPOSITORY": REPO,
           "GITHUB_API_URL": mock.url, "RELEASE_UPLOADS_URL": mock.url, "HOME": os.environ.get("HOME", "/tmp")}
    env.update(env_extra or {})
    mock.log.clear()
    p = subprocess.run([SCRIPT, tag, notes] + assets, env=env, capture_output=True, text=True, timeout=120)
    return p


def calls(mock):
    return [(e["method"], e["path"]) for e in mock.log]


def main():
    for tool in ("curl", "jq"):
        if shutil.which(tool) is None:
            print("test_release_publish: SKIPPED: no %s" % tool)
            return 0
    tmp = tempfile.mkdtemp(prefix="tsdb-release-publish.")
    try:
        return scenarios(tmp)
    finally:
        shutil.rmtree(tmp)


def scenarios(tmp):
    notes = os.path.join(tmp, "notes.md")
    text = "tsdb 1.2.3 notes\n\n- \"quotes\", a backslash \\ and $(not a command) `nor this`\n- ok\n"
    with open(notes, "w") as f:
        f.write(text)

    def mkassets(version, payload):
        tar = os.path.join(tmp, "tsdb-%s-x86_64-linux.tar.gz" % version)
        with open(tar, "wb") as f:
            f.write(payload)
        sha = tar + ".sha256"
        with open(sha, "w") as f:
            f.write("%s  %s\n" % (hashlib.sha256(payload).hexdigest(), os.path.basename(tar)))
        return [tar, sha]

    def contents(path):
        with open(path, "rb") as f:
            return f.read()

    base = "/repos/" + REPO
    mock = MockGitHub()
    try:
        # 1. A new tag: no release yet. Draft (aimed at the commit built), both assets, then published.
        assets = mkassets("1.2.3", os.urandom(300000))
        p = run(mock, "v1.2.3", notes, assets, {"RELEASE_COMMIT": COMMIT})
        check(p.returncode == 0, "fresh release: exit %d\n%s%s" % (p.returncode, p.stdout, p.stderr))
        c = calls(mock)
        want = [("GET", base + "/releases/tags/v1.2.3"), ("GET", base + "/releases"), ("POST", base + "/releases")]
        check(c[:3] == want, "fresh release: first calls %s" % c[:3])
        create = mock.log[2].get("json", {})
        check(create == {"tag_name": "v1.2.3", "name": "tsdb 1.2.3", "body": text, "draft": True,
                         "prerelease": False, "target_commitish": COMMIT}, "fresh release: create body %s" % create)
        check("::warning::" not in p.stdout, "fresh release: a warning: %s" % p.stdout)
        rel = mock.releases[0]
        ups = [e for e in mock.log if e["method"] == "POST" and e["path"].endswith("/assets")]
        check([e["query"] for e in ups] == ["name=" + os.path.basename(a) for a in assets],
              "fresh release: uploads %s" % [e["query"] for e in ups])
        check([e["ctype"] for e in ups] == ["application/gzip", "text/plain"], "fresh release: content types")
        check(all(e["sha256"] == hashlib.sha256(contents(a)).hexdigest() for e, a in zip(ups, assets)),
              "fresh release: uploaded bytes differ from the files")
        check(c[-1] == ("PATCH", base + "/releases/%d" % rel["id"]), "fresh release: last call %s" % (c[-1],))
        check(mock.log[-1].get("json") == {"draft": False, "prerelease": False, "make_latest": "legacy"},
              "fresh release: publish body %s" % mock.log[-1].get("json"))
        check(not any(e["method"] == "DELETE" for e in mock.log), "fresh release: deleted something")
        check(all(e["auth"] == "Bearer " + TOKEN and e["apiver"] == "2022-11-28"
                  and e["accept"] == "application/vnd.github+json" for e in mock.log), "fresh release: headers")
        check(not rel["draft"] and len(rel["assets"]) == 2, "fresh release: final state %s" % rel)
        check(TOKEN not in p.stdout + p.stderr, "fresh release: the token was printed")

        # 2. A rerun with a rebuilt tarball on the published release: refused without REPLACE_ASSETS (whoever
        #    downloaded it holds the old checksum), nothing changed.
        old = sorted((a["id"], a["name"], a["data"]) for a in rel["assets"])
        rel["body"] = "edited by hand on the release page"
        assets = mkassets("1.2.3", os.urandom(1000))
        p = run(mock, "v1.2.3", notes, assets)
        check(p.returncode != 0 and "replaced only on request" in p.stderr, "rerun, no replace: %r" % p.stderr)
        check(not any(m in ("POST", "DELETE", "PATCH") for m, _ in calls(mock)), "rerun, no replace: %s" % calls(mock))
        check(sorted((a["id"], a["name"], a["data"]) for a in rel["assets"]) == old, "rerun, no replace: assets")

        # 2b. With REPLACE_ASSETS=1: both new files are stored (as NAME.new) before any old one is deleted, then
        #     renamed; the published release's text, name and flags are left alone (no PATCH of the release, no
        #     POST of one), with a warning that its text is not ours.
        p = run(mock, "v1.2.3", notes, assets, {"REPLACE_ASSETS": "1"})
        check(p.returncode == 0, "rerun: exit %d\n%s%s" % (p.returncode, p.stdout, p.stderr))
        c = calls(mock)
        check(c[0] == ("GET", base + "/releases/tags/v1.2.3") and ("GET", base + "/releases") not in c,
              "rerun: lookup %s" % c)
        dels = sorted(int(path.rsplit("/", 1)[1]) for m, path in c if m == "DELETE")
        check(dels == sorted(i for i, _, _ in old), "rerun: deleted %s, want %s" % (dels, old))
        ups = [i for i, e in enumerate(mock.log) if e["method"] == "POST" and e["path"].endswith("/assets")]
        first_del = min(i for i, e in enumerate(mock.log) if e["method"] == "DELETE")
        check(len(ups) == 2 and max(ups) < first_del, "rerun: an old file was deleted before both new ones were up")
        check([mock.log[i]["query"] for i in ups] == ["name=%s.new" % os.path.basename(a) for a in assets],
              "rerun: uploads %s" % [mock.log[i]["query"] for i in ups])
        check(not any(path == base + "/releases/%d" % rel["id"] or path == base + "/releases" and m == "POST"
                      for m, path in c), "rerun: changed the release itself: %s" % c)
        check(len(mock.releases) == 1 and rel["body"] == "edited by hand on the release page", "rerun: release text")
        check("::warning::the release of v1.2.3 keeps its text" in p.stdout, "rerun: no warning about the text")
        check(sorted((a["name"], a["data"]) for a in rel["assets"])
              == sorted((os.path.basename(a), contents(a)) for a in assets), "rerun: assets are not the new files")

        # 2c. A replace whose upload fails: the old files stay as they were (nothing deleted).
        old = sorted((a["id"], a["name"], a["data"]) for a in rel["assets"])
        mock.fail[("POST", base + r"/releases/\d+/assets")] = 502
        p = run(mock, "v1.2.3", notes, mkassets("1.2.3", b"never stored"), {"REPLACE_ASSETS": "1"})
        check(p.returncode != 0 and not any(m == "DELETE" for m, _ in calls(mock)), "replace, failed upload: %s"
              % calls(mock))
        check(sorted((a["id"], a["name"], a["data"]) for a in rel["assets"]) == old, "replace, failed upload: assets")

        # 2d. A replace whose rename fails: the run fails with NAME.new left; the next run clears it and finishes.
        mock.fail[("PATCH", base + r"/releases/assets/\d+")] = 422
        assets = mkassets("1.2.3", b"third build")
        p = run(mock, "v1.2.3", notes, assets, {"REPLACE_ASSETS": "1"})
        check(p.returncode != 0 and "renaming" in p.stderr, "replace, failed rename: %r" % p.stderr)
        check(any(a["name"].endswith(".new") for a in rel["assets"]), "replace, failed rename: %s"
              % [a["name"] for a in rel["assets"]])
        p = run(mock, "v1.2.3", notes, assets, {"REPLACE_ASSETS": "1"})
        check(p.returncode == 0, "after a failed rename: exit %d\n%s%s" % (p.returncode, p.stdout, p.stderr))
        check(sorted((a["name"], a["data"]) for a in rel["assets"])
              == sorted((os.path.basename(a), contents(a)) for a in assets), "after a failed rename: %s"
              % [a["name"] for a in rel["assets"]])

        # 2e. A published release made by hand, without our files: they are added without REPLACE_ASSETS.
        hand = mock.add_release("v1.2.4", draft=False, body=text)
        assets = mkassets("1.2.4", b"tarball 1.2.4")
        p = run(mock, "v1.2.4", notes, assets)
        check(p.returncode == 0, "hand-made release: exit %d\n%s%s" % (p.returncode, p.stdout, p.stderr))
        check(sorted(a["name"] for a in hand["assets"]) == sorted(os.path.basename(a) for a in assets)
              and "::warning::" not in p.stdout, "hand-made release: %s %s" % (hand["assets"], p.stdout))

        # 2f. KEEP_DRAFT (the v4 code did not run): uploaded, left a draft, with a warning; a later run without it
        #     replaces the draft's files (no REPLACE_ASSETS needed: nobody has seen a draft) and publishes.
        assets = mkassets("1.2.5", b"tarball 1.2.5")
        p = run(mock, "v1.2.5", notes, assets, {"KEEP_DRAFT": "1"})
        r125 = [r for r in mock.releases if r["tag_name"] == "v1.2.5"]
        check(p.returncode == 0 and len(r125) == 1 and r125[0]["draft"] and len(r125[0]["assets"]) == 2
              and "stays a draft" in p.stdout, "keep draft: %d %s\n%s%s" % (p.returncode, r125, p.stdout, p.stderr))
        check(not any(m == "PATCH" for m, _ in calls(mock)), "keep draft: published")
        assets = mkassets("1.2.5", b"tarball 1.2.5, again")
        p = run(mock, "v1.2.5", notes, assets)
        check(p.returncode == 0 and not r125[0]["draft"] and sorted((a["name"], a["data"]) for a in r125[0]["assets"])
              == sorted((os.path.basename(a), contents(a)) for a in assets), "keep draft, then publish: %s\n%s%s"
              % (r125, p.stdout, p.stderr))
        p = run(mock, "v1.2.6", notes, mkassets("1.2.6", b"x"), {"RELEASE_COMMIT": "main"})
        check(p.returncode != 0 and not mock.log, "bad RELEASE_COMMIT: exit %d, %d requests" % (p.returncode,
                                                                                             len(mock.log)))

        # 3. A prerelease tag: prerelease true, never "latest".
        assets = mkassets("2.0.0-rc.1", b"rc tarball")
        p = run(mock, "v2.0.0-rc.1", notes, assets)
        check(p.returncode == 0, "prerelease: exit %d\n%s%s" % (p.returncode, p.stdout, p.stderr))
        create = [e for e in mock.log if e["method"] == "POST" and e["path"] == base + "/releases"]
        check(len(create) == 1 and create[0]["json"]["prerelease"] is True
              and create[0]["json"]["name"] == "tsdb 2.0.0-rc.1", "prerelease: create %s" % create)
        check(mock.log[-1].get("json") == {"draft": False, "prerelease": True, "make_latest": "false"},
              "prerelease: publish body %s" % mock.log[-1].get("json"))

        # 4. A draft an earlier, failed run left (with one asset of ours): completed, not duplicated.
        draft = mock.add_release("v1.3.0", draft=True, assets=[("tsdb-1.3.0-x86_64-linux.tar.gz", b"half")])
        stale = draft["assets"][0]["id"]
        assets = mkassets("1.3.0", b"whole tarball")
        n_before = len(mock.releases)
        p = run(mock, "v1.3.0", notes, assets)
        check(p.returncode == 0, "draft: exit %d\n%s%s" % (p.returncode, p.stdout, p.stderr))
        c = calls(mock)
        check(len(mock.releases) == n_before, "draft: a second release was made")
        check(("DELETE", base + "/releases/assets/%d" % stale) in c, "draft: the stale asset stayed: %s" % c)
        check(c[-1] == ("PATCH", base + "/releases/%d" % draft["id"]) and not draft["draft"], "draft: not published")
        check(len(draft["assets"]) == 2, "draft: assets %s" % [a["name"] for a in draft["assets"]])

        # 5. An upload that fails: the script fails and the release stays a draft (nobody sees it half done);
        #    a rerun finds that draft and finishes it.
        assets = mkassets("1.4.0", b"tarball 1.4.0")
        mock.fail[("POST", base + r"/releases/\d+/assets")] = 502
        p = run(mock, "v1.4.0", notes, assets)
        check(p.returncode != 0, "failed upload: exit 0")
        check("HTTP 502" in p.stderr, "failed upload: message %r" % p.stderr)
        r14 = [r for r in mock.releases if r["tag_name"] == "v1.4.0"]
        check(len(r14) == 1 and r14[0]["draft"], "failed upload: release %s" % r14)
        check(not any(m == "PATCH" for m, _ in calls(mock)), "failed upload: published anyway")
        p = run(mock, "v1.4.0", notes, assets)
        check(p.returncode == 0, "rerun after a failed upload: exit %d\n%s%s" % (p.returncode, p.stdout, p.stderr))
        r14 = [r for r in mock.releases if r["tag_name"] == "v1.4.0"]
        check(len(r14) == 1 and not r14[0]["draft"] and len(r14[0]["assets"]) == 2,
              "rerun after a failed upload: %s" % r14)

        # 6. Tags that are not release tags, and a repository name with a path in it: refused before any request.
        for bad in ("v1.2", "1.2.3", "v1.2.3;id", "v1.2.3-", "v1.2.3/../x", "v1.2.3\n", "v1.2.3-rc 1"):
            p = run(mock, bad, notes, assets)
            check(p.returncode != 0 and not mock.log, "bad tag %r: exit %d, %d requests" % (bad, p.returncode,
                                                                                          len(mock.log)))
        p = run(mock, "v1.5.0", notes, assets, {"GITHUB_REPOSITORY": "acme/tsdb/../x"})
        check(p.returncode != 0 and not mock.log, "bad repository: exit %d, %d requests" % (p.returncode,
                                                                                         len(mock.log)))

        # 7. An upload_url to another host: refused, the token goes nowhere else and nothing is uploaded.
        mock.evil_upload = True
        assets = mkassets("1.6.0", b"tarball 1.6.0")
        p = run(mock, "v1.6.0", notes, assets)
        mock.evil_upload = False
        check(p.returncode != 0 and "unexpected upload_url" in p.stderr, "foreign upload_url: %r" % p.stderr)
        check(not any(path.endswith("/assets") for m, path in calls(mock) if m == "POST"), "foreign upload_url")

        # 8. A wrong token: the API's message, not a silent success.
        p = run(mock, "v1.7.0", notes, mkassets("1.7.0", b"x"), {"GITHUB_TOKEN": "wrong"})
        check(p.returncode != 0 and "Bad credentials" in p.stderr, "wrong token: %r" % p.stderr)

        # 9. Immutable releases: a published release's assets cannot be replaced; the rerun says why and fails, and
        #    the files stay.
        mock.immutable = True
        old = sorted((a["id"], a["name"]) for a in rel["assets"])
        p = run(mock, "v1.2.3", notes, mkassets("1.2.3", b"again"), {"REPLACE_ASSETS": "1"})
        mock.immutable = False
        check(p.returncode != 0 and "immutable" in p.stderr, "immutable: %r" % p.stderr)
        check(sorted((a["id"], a["name"]) for a in rel["assets"]) == old, "immutable: assets changed")
    finally:
        mock.stop()

    if failures:
        print("test_release_publish: %d FAILED" % failures)
        return 1
    print("test_release_publish: all passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
