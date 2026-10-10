from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from hypothesis import given, strategies as st

from srdq.common import canonical as c

SEED = "00000000000000000000000000000000"


def test_c1_base_vectors():
    assert c.encode_val("STRING", None).hex() == "0004000000000000000000"
    assert c.encode_val("STRING", "").hex() == "0004010000000000000000"
    assert c.encode_val("STRING", "A").hex() == "000401000000000000000141"
    assert c.encode_val("INTEGER", 0).hex() == "000201000000000000000130"
    assert c.encode_val("DECIMAL", Decimal("1.20"), scale=2).hex() == "0003010000000000000003313230"
    assert c.EMPTY_GROUP_KEY.hex() == "535244512d4b45592d310000000000"
    assert c.sha256(b"abc").hex() == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


@pytest.mark.parametrize("key,bucket", [
    (-9223372036854775808, 510), (-1, 121), (0, 639), (1, 733),
    (9007199254740993, 141), (9223372036854775807, 855), (1570, 0), (2715, 0), (3949, 0),
])
def test_s1_fixed_vectors(key, bucket):
    k = c.single_key("INTEGER", key)
    assert c.s1_bucket(k, SEED) == bucket
    assert c.s1_selected(k, SEED) == (bucket == 0)


def test_s1_full_vectors():
    k = c.single_key("INTEGER", 1570)
    assert k.hex() == "535244512d4b45592d310000000001000201000000000000000431353730"
    assert c.s1_digest(k, SEED).hex() == "0540e835332c7d1001972b3e4b92fd01b21803dc43f25c39571bb19cb009c595"
    k = c.single_key("INTEGER", -9223372036854775808)
    assert k.hex() == ("535244512d4b45592d31000000000100020100000000000000142d39323233333732303336383534373735383038")
    assert c.s1_digest(k, SEED).hex() == "81acc252ccc59e0ed84d8106877f5fb9302f4e1104e6b5a59fd6ae3577074887"


def test_s1_hit_count_1_to_20000():
    hits = [i for i in range(1, 20001) if c.s1_selected(c.single_key("INTEGER", i), SEED)]
    assert len(hits) == 23
    assert {1570, 2715, 3949} <= set(hits)


def test_s1_reference_implementation_matches():
    import hashlib, struct

    def ref(integer_key, seed_hex):
        payload = str(integer_key).encode("ascii")
        v = struct.pack(">HBQ", 2, 1, len(payload)) + payload
        key = b"SRDQ-KEY-1\0" + struct.pack(">I", 1) + v
        wire = b"SRDQ-SAMPLE-1|" + seed_hex.encode("ascii") + b"|" + key.hex().encode("ascii")
        digest = hashlib.sha256(wire).digest()
        return key.hex(), digest.hex(), int.from_bytes(digest[:8], "big") % 1000

    for k in (-5, 0, 77, 2**63 - 1, -(2**63), 2**100):
        kb = c.single_key("INTEGER", k)
        assert (kb.hex(), c.s1_digest(kb, SEED).hex(), c.s1_bucket(kb, SEED)) == ref(k, SEED)


def test_seed_validation():
    with pytest.raises(c.CanonicalError):
        c.s1_digest(b"x", "00000000000000000000000000000000".upper().replace("0", "A"))
    with pytest.raises(c.CanonicalError):
        c.s1_digest(b"x", "0" * 31)


def test_d1_empty_vector():
    assert c.d1(b"\0" * 32, []).hex() == "e0e81fb9217e6a2d732789e74028f508b360b1855bd0528b07327ec22c158f72"


def test_string_key_bytes_length():
    k = c.single_key("STRING", "中文")
    vals = c.decode_key(k)
    assert vals == [(4, "中文".encode())]
    assert c.encode_val("STRING", "中文")[3:11] == (6).to_bytes(8, "big")


def test_null_key_rejected():
    with pytest.raises(c.CanonicalError):
        c.single_key("INTEGER", None)


def test_decimal_no_rounding():
    assert c.decimal_scaled_text("1.2", 2) == "120"
    assert c.decimal_scaled_text("-0.05", 2) == "-5"
    assert c.decimal_text("1.2", 2) == "1.20"
    assert c.decimal_text("-0.05", 2) == "-0.05"
    with pytest.raises(c.CanonicalError):
        c.decimal_scaled_text("1.234", 2)
    with pytest.raises(c.CanonicalError):
        c.decimal_scaled_text(1.2, 2)


def test_integer_text():
    assert c.canonical_int_text("0") == "0"
    assert c.canonical_int_text("-0") == "0"
    assert c.canonical_int_text(Decimal("12")) == "12"
    assert c.canonical_int_text("170141183460469231731687303715884105727") == "170141183460469231731687303715884105727"
    with pytest.raises(c.CanonicalError):
        c.canonical_int_text("1e3")
    with pytest.raises(c.CanonicalError):
        c.canonical_int_text(True)


def test_datetime_and_date():
    assert c.payload_for("DATE", dt.date(2026, 1, 2)) == b"2026-01-02"
    assert c.payload_for("DATETIME", dt.datetime(2026, 1, 2, 3, 4, 5, 120000), fsp=6) == b"2026-01-02T03:04:05.120000"
    assert c.payload_for("DATETIME", dt.datetime(2026, 1, 2, 3, 4, 5), fsp=0) == b"2026-01-02T03:04:05"
    with pytest.raises(c.CanonicalError):
        c.payload_for("DATETIME", dt.datetime(2026, 1, 2, 3, 4, 5, 1), fsp=0)


def test_canonical_json():
    assert c.canonical_json({"b": 1, "a": "中"}) == '{"a":"中","b":1}'.encode()
    with pytest.raises(c.CanonicalError):
        c.canonical_json({"a": 1.5})


def test_unsupported_types():
    with pytest.raises(c.CanonicalError):
        c.encode_val("JSON", "{}")
    with pytest.raises(c.CanonicalError):
        c.encode_val("FLOAT64", 1.0)


_values = st.one_of(
    st.none().map(lambda v: ("STRING", v)),
    st.text().map(lambda v: ("STRING", v)),
    st.integers().map(lambda v: ("INTEGER", v)),
    st.binary().map(lambda v: ("BINARY", v)),
)


@given(st.lists(_values, max_size=6), st.lists(_values, max_size=6))
def test_c1_injective(a, b):
    ra = c.row_bytes(c.encode_val(t, v) for t, v in a)
    rb = c.row_bytes(c.encode_val(t, v) for t, v in b)
    norm = lambda xs: [(t, v) for t, v in xs]
    if norm(a) != norm(b):
        assert ra != rb
    else:
        assert ra == rb


@given(st.lists(_values, max_size=6))
def test_c1_roundtrip(vals):
    k = c.key_bytes(c.encode_val(t, v) for t, v in vals)
    decoded = c.decode_key(k)
    assert len(decoded) == len(vals)
    for (tag, payload), (t, v) in zip(decoded, vals):
        assert tag == c.LOGICAL_TAGS[t]
        if v is None:
            assert payload is None
