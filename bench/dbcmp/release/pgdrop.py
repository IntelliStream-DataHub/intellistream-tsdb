# SPDX-License-Identifier: AGPL-3.0-only
"""pgdrop.py PORTFILE: a fake PostgreSQL server for harnesstest.sh. It accepts one connection, answers the startup
(no SSL, AuthenticationOk, ReadyForQuery), then closes it before any query: qdrive's next PQexec has no server."""
import socket, struct, sys

s = socket.socket(); s.bind(("127.0.0.1", 0)); s.listen(1)
open(sys.argv[1], "w").write(str(s.getsockname()[1]))
c, _ = s.accept()
c.settimeout(10)
def packet():
    n = struct.unpack("!I", c.recv(4, socket.MSG_WAITALL))[0]
    return c.recv(n - 4, socket.MSG_WAITALL)
p = packet()
if struct.unpack("!I", p[:4])[0] == 80877103:  # SSLRequest: refused, then the real startup message
    c.sendall(b"N"); packet()
c.sendall(b"R" + struct.pack("!II", 8, 0) + b"Z" + struct.pack("!I", 5) + b"I")
c.close(); s.close()
