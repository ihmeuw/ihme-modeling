import pytest
from pydisagg.ihme.schema import Schema, PrefixStatus


class TestSchema(Schema):
    val: str = "value"
    val_sd: str = "value_sd"

    @property
    def val_fields(self) -> list[str]:
        return ["val", "val_sd"]


def test_apply_prefix():
    schema = TestSchema(prefix="test_")
    rename_map = schema.apply_prefix()
    assert schema.val == "test_value"
    assert schema.val_sd == "test_value_sd"
    assert schema.prefix_status == PrefixStatus.PREFIXED
    assert rename_map == {"value": "test_value", "value_sd": "test_value_sd"}


def test_apply_prefix_already_applied():
    schema = TestSchema(prefix="test_", prefix_status=PrefixStatus.PREFIXED)
    with pytest.raises(ValueError, match="prefix already applied"):
        schema.apply_prefix()


def test_remove_prefix():
    schema = TestSchema(
        prefix="test_",
        val="test_value",
        val_sd="test_value_sd",
        prefix_status=PrefixStatus.PREFIXED,
    )
    schema.remove_prefix()
    assert schema.val == "value"
    assert schema.val_sd == "value_sd"
    assert schema.prefix_status == PrefixStatus.NOT_PREFIXED


def test_remove_prefix_not_applied():
    schema = TestSchema(prefix="test_")
    schema.remove_prefix()
    assert schema.val == "value"
    assert schema.val_sd == "value_sd"
    assert schema.prefix_status == PrefixStatus.NOT_PREFIXED


def test_columns_property():
    schema = TestSchema()
    assert schema.columns == []


def test_val_fields_property():
    schema = TestSchema()
    assert schema.val_fields == ["val", "val_sd"]
