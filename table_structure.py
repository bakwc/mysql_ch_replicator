from dataclasses import dataclass, field

@dataclass
class TableField:
    name: str = ''
    field_type: str = ''
    parameters: str = ''

@dataclass
class TableStructure:
    fields: list[TableField] = field(default_factory=list)
    primary_key: str = ''
    primary_key_idx: int = 0

    def preprocess(self):
        field_names = [f.name for f in self.fields]
        self.primary_key_idx = field_names.index(self.primary_key)
