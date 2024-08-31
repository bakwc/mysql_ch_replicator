from dataclasses import dataclass, field

@dataclass
class TableField:
    name: str = ''
    field_type: str = ''
    parameters: str = ''

@dataclass
class TableStructure:
    fields: list = field(default_factory=list)
    primary_key: str = ''
    primary_key_idx: int = 0
    table_name: str = ''

    def preprocess(self):
        field_names = [f.name for f in self.fields]
        self.primary_key_idx = field_names.index(self.primary_key)

    def add_field_after(self, new_field: TableField, after: str):

        idx_to_insert = None
        for idx, field in enumerate(self.fields):
            if field.name == after:
                idx_to_insert = idx + 1

        if idx_to_insert is None:
            raise Exception('field after not found', after)

        self.fields.insert(idx_to_insert, new_field)

    def remove_field(self, field_name):
        for idx, field in enumerate(self.fields):
            if field.name == field_name:
                del self.fields[idx]
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
