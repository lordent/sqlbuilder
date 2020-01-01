from sql.types import TypeField, TypeTable, TypeQ, TypeQuery
from sql.query import Q


class Field(TypeField):

    foreign = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __set_name__(self, table: TypeTable, name):
        self.table = table
        self.name = name
        self.query_name = f'{table.query_alias}."{name}"'

    def __eq__(self, value):
        if isinstance(value, (tuple, list, set)):
            return Q(f'{self.query_name} = ANY({Q.PLACEHOLDER})', value)
        elif isinstance(value, TypeField):
            return Q(f'{self.query_name} = {value.query_name}')
        elif isinstance(value, TypeQuery):
            q = value.query()
            return Q(f'{self.query_name} IN ({q.query})', *q.args)
        elif value is None:
            return Q(f'{self.query_name} IS NULL')
        else:
            return Q(f'{self.query_name} = {Q.PLACEHOLDER}', value)

    def __ne__(self, value):
        if isinstance(value, (tuple, list, set)):
            return Q(f'{self.query_name} != ANY({Q.PLACEHOLDER})', value)
        elif isinstance(value, TypeField):
            return Q(f'{self.query_name} != {value.query_name}')
        elif isinstance(value, TypeQuery):
            q = value.query()
            return Q(f'{self.query_name} NOT IN ({q.query})', *q.args)
        elif value is None:
            return Q(f'{self.query_name} IS NOT NULL')
        else:
            return Q(f'{self.query_name} != {Q.PLACEHOLDER}', value)

    def like(self, value, mode=Q.LIKE_BOTH, method='LIKE'):
        if mode == Q.LIKE_BOTH:
            return Q(f"{self.query_name} {method} ('%' || {Q.PLACEHOLDER} || '%')", value)
        elif mode == Q.LIKE_START:
            return Q(f"{self.query_name} {method} ('%' || {Q.PLACEHOLDER})", value)
        elif mode == Q.LIKE_END:
            return Q(f"{self.query_name} {method} ({Q.PLACEHOLDER} || '%')", value)

    def ilike(self, value, mode=Q.LIKE_BOTH):
        return self.like(value, mode=mode, method='ILIKE')

    def to_char(self, value):
        return Q(f'TO_CHAR({self.query_name}, {Q.PLACEHOLDER})', value, field=self)

    def count(self):
        return Q(f'COUNT({self.query_name})', field=self)
