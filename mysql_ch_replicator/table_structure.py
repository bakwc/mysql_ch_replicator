from dataclasses import dataclass, field
from typing import Any


@dataclass
class TableField:
    name: str = ''
    field_type: str = ''
    parameters: str = ''
    additional_data: Any = None

@dataclass
class TableStructure:
    fields: list = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    primary_key_ids: int = 0
    table_name: str = ''
    charset: str = ''
    charset_python: str = ''
    if_not_exists: bool = False

    def preprocess(self):
        field_names = [f.name for f in self.fields]
        self.primary_key_ids = [
            field_names.index(key) for key in self.primary_keys
        ]

    def add_field_first(self, new_field: TableField):

        self.fields.insert(0, new_field)
        self.preprocess()

    def add_field_after(self, new_field: TableField, after: str):

        idx_to_insert = None
        for idx, field in enumerate(self.fields):
            if field.name == after:
                idx_to_insert = idx + 1

        if idx_to_insert is None:
            raise Exception('field after not found', after)

        self.fields.insert(idx_to_insert, new_field)
        self.preprocess()

    def remove_field(self, field_name):
        for idx, field in enumerate(self.fields):
            if field.name == field_name:
                del self.fields[idx]
                self.preprocess()
                return
        raise Exception(f'field {field_name} not found')

    def update_field(self, new_field: TableField):
        for idx, field in enumerate(self.fields):
            if field.name == new_field.name:
                self.fields[idx] = new_field
                return
        raise Exception(f'field {new_field.name} not found')

    def has_field(self, field_name):
        for field in self.fields:
            if field.name == field_name:
                return True
        return False

    def get_field(self, field_name):
        for field in self.fields:
            if field.name == field_name:
                return field
        return None
