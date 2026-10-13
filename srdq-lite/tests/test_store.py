import pytest

from srdq_lite.store import JsonlAppender, read_jsonl


def test_read_jsonl_ignores_and_repairs_torn_tail(tmp_path):
    path = tmp_path / "state.jsonl"
    path.write_bytes(b'{"id":1}\n{"id":')
    assert read_jsonl(path) == [{"id": 1}]
    with JsonlAppender(path) as out:
        out.write({"id": 2})
    assert read_jsonl(path) == [{"id": 1}, {"id": 2}]


def test_read_jsonl_rejects_corrupt_middle_line(tmp_path):
    path = tmp_path / "state.jsonl"
    path.write_text('{"id":1}\nbad\n{"id":2}\n')
    with pytest.raises(ValueError, match="corrupt line 2"):
        read_jsonl(path)
