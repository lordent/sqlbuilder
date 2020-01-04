from sql.types import TypeField, TypeTable, TypeQ, TypeQuery
from sql.query import Q


class Field(TypeField):

    foreign = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __set_name__(self, table: TypeTable, name):
        self.table = table
        self.name = name
        self.query = f'{table.Meta.query}."{name}"'

    def __eq__(self, value):
        if isinstance(value, (tuple, list, set)):
            return Q(f'{self.query} = ANY({Q.PLACEHOLDER})', value)
        elif isinstance(value, TypeField):
            return Q(f'{self.query} = {value.query}')
        elif isinstance(value, TypeQuery):
            q = value.query()
            return Q(f'{self.query} IN ({q.query})', *q.args)
        elif value is None:
            return Q(f'{self.query} IS NULL')
        else:
            return Q(f'{self.query} = {Q.PLACEHOLDER}', value)

    def __ne__(self, value):
        if isinstance(value, (tuple, list, set)):
            return Q(f'{self.query} != ANY({Q.PLACEHOLDER})', value)
        elif isinstance(value, TypeField):
            return Q(f'{self.query} != {value.query}')
        elif isinstance(value, TypeQuery):
            q = value.query()
            return Q(f'{self.query} NOT IN ({q.query})', *q.args)
        elif value is None:
            return Q(f'{self.query} IS NOT NULL')
        else:
            return Q(f'{self.query} != {Q.PLACEHOLDER}', value)

    def like(self, value, mode=Q.LIKE_BOTH, method='LIKE'):
        if mode == Q.LIKE_BOTH:
            return Q(f"{self.query} {method} ('%' || {Q.PLACEHOLDER} || '%')", value)
        elif mode == Q.LIKE_START:
            return Q(f"{self.query} {method} ('%' || {Q.PLACEHOLDER})", value)
        elif mode == Q.LIKE_END:
            return Q(f"{self.query} {method} ({Q.PLACEHOLDER} || '%')", value)

    def ilike(self, value, mode=Q.LIKE_BOTH):
        return self.like(value, mode=mode, method='ILIKE')

    def to_char(self, value):
        return Q(f'TO_CHAR({self.query}, {Q.PLACEHOLDER})', value, field=self)

    def count(self):
        return Q(f'COUNT({self.query})', field=self)

    def order(self, mode):
        mode = {
            0: Q.ASC,
            1: Q.DESC,
            '0': Q.ASC,
            '1': Q.DESC,
            '': Q.ASC,
            '-': Q.DESC,
            Q.ASC: Q.ASC,
            Q.DESC: Q.DESC,
        }.get(mode, Q.ASC)
        if mode:
            return Q(f'{self.query} {mode}')
        else:
            return self

    def asc(self):
        return self.order(Q.ASC)

    def desc(self):
        return self.order(Q.DESC)
