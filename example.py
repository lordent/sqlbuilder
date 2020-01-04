import asyncio
import uuid
import datetime
import decimal

import asyncpg

from sql import Table, Field, Select, Q
from sql.types import TypeTable


class User(Table):
    first_name = Field()
    last_name = Field()
    date_joined = Field()

    class Meta:
        name = 'users_user'


class Company(Table):
    name = Field()
    user_id = Field(foreign=User)

    class Meta:
        name = 'company_company'


async def main():
    conn = await asyncpg.connect('postgresql://test:test@0.0.0.0/test')

    stmt = (
        Select(
            # User.id,
            # uuid=uuid.uuid4(),
            # date=datetime.datetime.now().date(),
            # datetime=datetime.datetime.now(),
            # decimal=decimal.Decimal('1250.56'),
            # firstName=User.first_name,
            # lastName=User.last_name,
            companyName=Company.name,
        )
        # .distinct()
        .select(
            joined=User.date_joined.to_char('DD/MM/YYYY'),
        )
        .join(User[2], User.last_name == User[2].last_name)
        #@ .join(Company, Company.user_id == User.id, Select.JOIN_RIGHT)
        .where(
            User.id != User[2].id,
            # User.last_name == ('Васильев', 'Емельянов'),
            # Company.id != None,
            # User.date_joined.to_char('DD/MM/YYYY') == '08/12/2019',
            # User.id == (22, 33),
            # User.last_name.ilike('васильев') | User.last_name.like('Емель', Q.LIKE_END),
        )
        .group(
            User.id,
            firstName=User.first_name,
            lastName=User.last_name,
            companyId=Company.id,
        )
        .having(
            User.id.count() > 1,
        )
        .order(
            User.last_name,
        )
    )

    # print(str(stmt))

    for row in await conn.fetch(*stmt):
        print(dict(row))

    stmt = (
        Select()
        .group(
            firstName=User.first_name,
        )
        .having(
            User.id.count() > 1,
        )
    )

    # print(str(stmt))

    stmt = (
        Select(
            User.id,
            firstName=User.first_name,
            lastName=User.last_name,
        )
        .where(
            # User.last_name == 'Емельянов',
            User.first_name == stmt,
        )
        .order(
            User.first_name.order(Q.DESC),
            User.id.desc(),
        )[3:10]
    )

    # print(str(stmt))

    # print(list(stmt))

    for row in await conn.fetch(*stmt):
        print(dict(row))

asyncio.get_event_loop().run_until_complete(main())
