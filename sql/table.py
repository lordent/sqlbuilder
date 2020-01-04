from sql.field import Field
from sql.types import TypeTable


class BaseMeta:
    postfix = ''
    name = ''


class TableManager(type):

    def __new__(mcl, name, bases, attributes, **kwargs):
        fields = {}
        for base in bases:
            if hasattr(base, 'Meta') and hasattr(base.Meta, 'fields'):
                fields.update(base.Meta.fields)
        for attribute_name, value in attributes.items():
            if isinstance(value, Field):
                fields[attribute_name] = value

        for field_name, field in fields.items():
            fields[field_name] = attributes[field_name] = field.__class__(**field.__dict__)

        Meta = type('Meta', (BaseMeta, ), {
            'name': name.lower(),
        })

        if 'Meta' in attributes:
            Meta = type('Meta', (attributes['Meta'], Meta, ), {})

        Meta.query = f'{Meta.name}{Meta.postfix}'
        Meta.fields = fields

        attributes['Meta'] = Meta

        cls = super().__new__(mcl, name, bases, attributes, **kwargs)

        for field_name, field in cls.Meta.fields.items():
            field.__set_name__(cls, field_name)

        return cls

    def __getitem__(cls, i):
        attributes = cls.__dict__.copy()
        attributes['Meta'].postfix = i
        return type(cls).__new__(type(cls), cls.__name__, cls.__bases__, attributes)

    def __hash__(cls):
        return hash(cls.Meta.query)

    def __eq__(cls, value):
        if value and issubclass(value, TypeTable):
            return cls.Meta.query == value.Meta.query
        else:
            return False


class Table(TypeTable, metaclass=TableManager):
    id = Field()

    @classmethod
    def get_foreign(cls, table):
        for field in cls.Meta.fields.values():
            if field.foreign == table:
                return table.id == field

        for field in table.Meta.fields.values():
            if field.foreign == cls:
                return cls.id == field
