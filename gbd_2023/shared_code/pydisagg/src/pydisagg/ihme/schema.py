from enum import StrEnum

from pydantic import BaseModel


class PrefixStatus(StrEnum):
    PREFIXED = "prefixed"
    NOT_PREFIXED = "not_prefixed"


class Schema(BaseModel):
    prefix: str = ""
    prefix_status: PrefixStatus = PrefixStatus.NOT_PREFIXED

    @property
    def columns(self) -> list[str]:
        return []

    @property
    def val_fields(self) -> list[str]:
        return []

    def apply_prefix(self) -> dict[str, str]:
        if self.prefix_status == PrefixStatus.PREFIXED:
            raise ValueError("prefix already applied")
        rename_map = {}
        for field in self.val_fields:
            new_field_val = self.prefix + (field_val := getattr(self, field))
            rename_map[field_val] = new_field_val
            setattr(self, field, new_field_val)
        self.prefix_status = PrefixStatus.PREFIXED
        return rename_map

    def remove_prefix(self) -> None:
        for field in self.val_fields:
            setattr(self, field, getattr(self, field).removeprefix(self.prefix))
        self.prefix_status = PrefixStatus.NOT_PREFIXED
