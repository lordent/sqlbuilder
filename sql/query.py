import decimal
import uuid
import datetime

from sql.types import TypeQ, TypeField


class Q(TypeQ):
    PLACEHOLDER = '#'

    TYPE_INTEGER = 'integer'
    TYPE_BOOLEAN = 'boolean'
    TYPE_NUMERIC = 'numeric'
    TYPE_UUID = 'uuid'
    TYPE_TIMESTAMP = 'timestamp'
    TYPE_DATE = 'date'

    LIKE_BOTH = 'both'
    LIKE_START = 'start'
    LIKE_END = 'end'

    def __init__(self, query, *args, field=None):
        self.query = query
        self.args = args
        self.field = field

    def attach(self, args):
        args.extend(self.args)
        return self.query

    @classmethod
    def value(cls, value, value_type=None):
        if not value_type:
            if isinstance(value, int):
                value_type = cls.TYPE_INTEGER
            elif isinstance(value, bool):
                value_type = cls.TYPE_BOOLEAN
            elif isinstance(value, decimal.Decimal):
                value_type = cls.TYPE_NUMERIC
            elif isinstance(value, uuid.UUID):
                value_type = cls.TYPE_UUID
            elif isinstance(value, datetime.datetime):
                value_type = cls.TYPE_TIMESTAMP
            elif isinstance(value, datetime.date):
                value_type = cls.TYPE_DATE
        return cls(f'{cls.PLACEHOLDER}::{value_type}' if value_type else cls.PLACEHOLDER, value)

    def __or__(self, q: TypeQ):
        return self.__class__(f'({self.query} OR {q.query})', *self.args, *q.args)

    def __and__(self, q: TypeQ):
        return self.__class__(f'{self.query} AND {q.query}', *self.args, *q.args)

    def __eq__(self, value):
        if isinstance(value, (tuple, list, set)):
            return self.__class__(f'{self.query} = ANY({self.PLACEHOLDER})', *self.args, value)
        elif isinstance(value, TypeField):
            return self.__class__(f'{self.query} = {value.query_name}')
        else:
            return self.__class__(f'{self.query} = {self.PLACEHOLDER}', *self.args, value)

    def __gt__(self, q):
        if isinstance(q, TypeQ):
            return self.__class__(f'{self.query} > {q.query}', *self.args, *q.args)
        elif isinstance(q, TypeField):
            return self.__class__(f'{self.query} > {q.query_name}', *self.args)
        else:
            return self.__class__(f'{self.query} > {self.PLACEHOLDER}', *self.args, q)

    def __ge__(self, q):
        if isinstance(q, TypeQ):
            return self.__class__(f'{self.query} >= {q.query}', *self.args, *q.args)
        elif isinstance(q, TypeField):
            return self.__class__(f'{self.query} >= {q.query_name}', *self.args)
        else:
            return self.__class__(f'{self.query} >= {self.PLACEHOLDER}', *self.args, q)
