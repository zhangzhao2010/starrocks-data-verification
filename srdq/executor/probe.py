"""S1 pushdown capability probe: compile the sample predicate on literal vectors and compare stage by stage."""
from __future__ import annotations

from ..common import canonical as c
from . import compiler
from .adapter.base import AdapterError

PROBE_SEED = "00112233445566778899aabbccddeeff"
INTEGER_VECTORS = [0, 1, -1, 7, 42, 1000, 123456789, -123456789, 2**31 - 1, -(2**31), 2**63 - 1, -(2**63)]
STRING_VECTORS = ["", "A", "a b ", "订单-001", "😀x", "x" * 200]


def expected(key_type: str, value, seed_hex: str, bucket_count: int) -> tuple[str, str, int]:
    k = c.single_key(key_type, value)
    return k.hex(), c.s1_digest(k, seed_hex).hex(), c.s1_bucket(k, seed_hex, bucket_count)


def run_probe(adapter, *, seed_hex: str = PROBE_SEED, bucket_count: int = 1000) -> dict:
    failures = []
    vectors = [("INTEGER", v) for v in INTEGER_VECTORS] + [("STRING", v) for v in STRING_VECTORS]
    try:
        for key_type, value in vectors:
            want = expected(key_type, value, seed_hex, bucket_count)
            sql, params = compiler.compile_s1_probe(key_type, value, seed_hex, bucket_count)
            row = adapter.scalar_row(sql, params)
            got = (str(row[0]).lower(), str(row[1]).lower(), int(row[2]))
            for stage, w, g in zip(("key_hex", "sha256", "bucket"), want, got):
                if w != g:
                    failures.append({"key_type": key_type, "value": str(value)[:40], "stage": stage,
                                     "expected": str(w)[:80], "got": str(g)[:80]})
                    break
    except AdapterError as e:
        return {"status": "PROBE_ERROR", "pass": False, "error_class": e.error_class, "message": str(e)[:500],
                "vectors_digest": vectors_digest(seed_hex, bucket_count), "failures": failures}
    return {"status": "PASS" if not failures else "UNSUPPORTED_SAMPLE_PUSHDOWN", "pass": not failures,
            "vectors_digest": vectors_digest(seed_hex, bucket_count), "failures": failures[:20],
            "key_types": ["INTEGER", "STRING"]}


def vectors_digest(seed_hex: str, bucket_count: int) -> str:
    return c.json_digest({"seed": seed_hex, "buckets": str(bucket_count),
                          "integer": [str(v) for v in INTEGER_VECTORS], "string": STRING_VECTORS,
                          "compiler": COMPILER_RELEASE}).hex()


COMPILER_RELEASE = "srdq-compiler-1"
