from sql.field import Field
from sql.types import TypeTable


class TableManager(type):

    def __new__(mcl, name, bases, attributes, **kwargs):
        fields = {}
        for base in bases:
            if hasattr(base, 'fields'):
                fields.update(base.fields)
        for attribute_name, value in attributes.items():
            if isinstance(value, Field):
                fields[attribute_name] = value

        for field_name, field in fields.items():
            fields[field_name] = attributes[field_name] = field.__class__(**field.__dict__)

        attributes.setdefault('i', 1)
        attributes.setdefault('query_name', name.lower())

        attributes['fields'] = fields
        attributes['query_alias'] = f'{attributes["query_name"]}{attributes["i"]}'

        cls = super().__new__(mcl, name, bases, attributes, **kwargs)

        for field_name, field in cls.__dict__['fields'].items():
            field.__set_name__(cls, field_name)

        return cls

    def __getitem__(cls, i):
        attributes = cls.__dict__.copy()
        attributes['i'] = i
        return type(cls).__new__(type(cls), cls.__name__, cls.__bases__, attributes)

    def __hash__(self):
        return hash(self.query_alias)

    def __eq__(self, value):
        return self.query_alias == value.query_alias if isinstance(value, TypeTable) else value


class Table(TypeTable, metaclass=TableManager):
    id = Field()

    @classmethod
    def get_foreign(cls, table: TypeTable):
        for field in cls.fields.values():
            if field.foreign == table:
                return table.id == field

        for field in table.fields.values():
            if field.foreign == cls:
                return cls.id == field
