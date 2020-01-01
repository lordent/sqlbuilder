from sql.types import TypeField, TypeQ, TypeQuery
from sql.query import Q


class Select(TypeQuery):
    JOIN_lEFT = 'JOIN'
    JOIN_RIGHT = 'RIGHT JOIN'

    def __init__(self, *args, **kwargs):
        self.method = 'SELECT'
        self.params = {
            'from': set(),
            'fields': [],
            'join': {},
            'where': [],
            'group': [],
            'having': [],
            'offset': None,
            'limit': None
        }
        self.agrs = []
        self.select(*args, **kwargs)

    def select(self, *args, **kwargs):
        for q in args:
            if isinstance(q, TypeField):
                if q.table:
                    self.join(q.table)
                self.params['fields'].append((q, None))

        for alias, q in kwargs.items():
            if isinstance(q, TypeField):
                if q.table:
                    self.join(q.table)
            elif isinstance(q, TypeQ):
                if q.field and q.field.table:
                    self.join(q.field.table)
            self.params['fields'].append((q, alias))

        return self

    def distinct(self):
        self.method = 'SELECT DISTINCT'
        return self

    def join(self, table, q=None, method=JOIN_lEFT):
        if q:
            self.params['join'][table] = q, method
            self.params['from'].discard(table)
        else:
            for from_table in self.params['from']:
                if table not in self.params['join'] and table not in self.params['from']:
                    q = from_table.get_foreign(table)
                    if q:
                        return self.join(table, q)
            if table not in self.params['join'] and table not in self.params['from']:
                self.params['from'].add(table)
        return self

    def where(self, *q):
        self.params['where'].extend(q)
        return self

    def group(self, *args, **kwargs):
        self.select(*args, **kwargs)
        self.params['group'].extend(args)
        self.params['group'].extend(kwargs.values())
        return self

    def having(self, *q):
        self.params['having'].extend(q)
        return self

    def query(self):
        args = []

        fields = []
        for q, alias in self.params['fields']:
            if isinstance(q, TypeQ):
                fields.append((q.attach(args), alias))
            elif isinstance(q, TypeField):
                fields.append((q.query_name, alias))
            else:
                fields.append((Q.value(q).attach(args), alias))

        chunks = [
            self.method,
            ', '.join(
                f'{query} "{alias}"' if alias else query
                for query, alias in fields
            ),
            'FROM',
            ', '.join(
                f'{table.query_name} {table.query_alias}'
                for table in self.params['from']
            )
        ]

        for table, (q, method) in self.params['join'].items():
            chunks.append(' '.join((
                method,
                table.query_name,
                table.query_alias,
                'ON',
                q.attach(args)
            )))

        if self.params['where']:
            chunks.append('WHERE')
            chunks.append(' AND '.join((
                q.attach(args)
                for q in self.params['where']
            )))

        if self.params['group']:
            chunks.append('GROUP BY')
            chunks.append(', '.join(
                q.query_name if isinstance(q, TypeField) else q.attach(args)
                for q in self.params['group']
            ))

        if self.params['having']:
            chunks.append('HAVING')
            chunks.append(', '.join(
                q.query_name if isinstance(q, TypeField) else q.attach(args)
                for q in self.params['having']
            ))

        if self.params['offset'] is not None:
            chunks.append(Q(f'OFFSET {Q.PLACEHOLDER}', self.params['offset']).attach(args))

        if self.params['limit'] is not None:
            chunks.append(Q(f'LIMIT {Q.PLACEHOLDER}', self.params['limit']).attach(args))

        return Q(' '.join(chunks), *args)

    def __getitem__(self, key):
        if isinstance(key, slice):
            self.params['offset'] = key.start
            self.params['limit'] = key.stop
        else:
            self.params['limit'] = key
        return self

    def __iter__(self):
        q = self.query()
        sql = q.query

        for i in range(1, len(q.args) + 1):
            sql = sql.replace(Q.PLACEHOLDER, f'${i}', 1)

        yield sql
        yield from q.args

    def __str__(self):
        stmt = iter(self)
        sql = next(stmt)
        for i, arg in enumerate(stmt, start=1):
            sql = sql.replace(f'${i}', str(arg))
        return sql
