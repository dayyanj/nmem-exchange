"""Generate an nmem-exchange agent identity.

  python -m nmem_exchange.keygen <agent_id> <keyfile_out.json>

Writes the PRIVATE keyfile (chmod 600 — never commit, never share) and prints the
PUBLIC bundle to paste into peers' rosters / allowlists.
"""
import json
import os
import sys

from nmem_exchange import crypto


def main():
    if len(sys.argv) != 3:
        print("usage: python -m nmem_exchange.keygen <agent_id> <keyfile.json>")
        sys.exit(2)
    agent_id, keyfile = sys.argv[1], sys.argv[2]
    idn = crypto.generate_identity(agent_id)
    # Create the keyfile ATOMICALLY at 0600 before writing any secret: O_EXCL
    # refuses to clobber an existing path, O_NOFOLLOW refuses a symlinked path,
    # and the mode means it's never briefly group/world-readable.
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(keyfile, flags, 0o600)
    except FileExistsError:
        print(f"refusing to overwrite existing keyfile: {keyfile} (delete it first to regenerate)")
        sys.exit(1)
    with os.fdopen(fd, "w") as f:
        json.dump(idn.export_secret(), f, indent=2)
    print(f"wrote PRIVATE keyfile: {keyfile} (0600) — do not commit or share")
    print("\nPUBLIC bundle (add to each peer's roster/allowlist):")
    print(json.dumps(idn.public_bundle(), indent=2))


if __name__ == "__main__":
    main()
