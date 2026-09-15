#!/usr/bin/env bash
# Codex correctness+security review for nmem-exchange. Run between slices.
#   ./tools/codex_review.sh nmem_exchange/crypto.py
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$REPO/tools/review_findings.json"
SCHEMA="$REPO/tools/review_schema.json"
FOCUS="${*:-nmem_exchange/ (the whole package)}"

cd "$REPO"
codex exec --sandbox read-only --skip-git-repo-check \
  --output-schema "$SCHEMA" -o "$OUT" --color never <<PROMPT
You are doing a CORRECTNESS + SECURITY review of nmem-exchange — an end-to-end-encrypted
message bus for AI agents. The crypto is the security boundary, so scrutinise it hard.

Design intent (do NOT flag as bugs): PyNaCl. Ed25519 for signatures/authorship, X25519 for
encryption. ENCRYPTED channels: body sealed once with a random SecretBox key, that key wrapped
to each recipient via anonymous SealedBox, whole envelope signed (Ed25519). OPEN channels:
signed plaintext. Inbound authenticated against an allowlist of trusted sender public keys.
The broker (Redis) is untrusted and must only ever see ciphertext + wrapped keys.

Review focus: ${FOCUS}

Look specifically for REAL security/correctness defects:
- Does the signature cover ALL security-relevant fields (from, channel, kind, msg_id, in_reply_to,
  ts, ct/pt, keys)? Any field an attacker could change WITHOUT invalidating the signature?
- Nonce handling / any nonce reuse (SecretBox.encrypt manages its own nonce — is that relied on correctly?).
- Key confusion between signing (Ed25519) and box (X25519) keys.
- Is authorship actually enforced (SealedBox is anonymous — is the Ed25519 sig the only authorship proof, and is it checked BEFORE decrypt)?
- allowlist bypass: any path where an unknown/unsigned sender is accepted, or verify_sig returns
  True on malformed input.
- base64/JSON canonicalisation issues that could let two different envelopes share a signature,
  or signature-stripping.
- Exception handling that could turn a crypto failure into a silent accept.
- Replay: note (don't necessarily fail) whether anything at this layer dedups replays.
- Private-key exposure: does export_secret / logging ever leak private keys inadvertently?

Ignore style/naming/docstrings. For each finding give file, line, severity, category, the concrete
issue, and a concrete fix. If clean, return {"findings": []}.
PROMPT
echo "findings -> $OUT"
python3 -c "import json;print(len(json.load(open('$OUT'))['findings']),'findings')" 2>/dev/null || true
