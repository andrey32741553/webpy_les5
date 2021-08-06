import hashlib
import datetime
import os
from functools import partial

from flask_jwt_extended import create_access_token
from aiohttp import web
from aiopg import pool
from aiopg.sa import create_engine
import sqlalchemy as sa

routes = web.RouteTableDef()
app = web.Application()
SQLALCHEMY_DATABASE_URI = 'postgresql://{}:{}@localhost:5432/ad_api'.format(os.getenv('DB_USER'),
                                                                            os.getenv('DB_PASSWORD'))
SALT = 'my_salt'
metadata = sa.MetaData()

ads_table = sa.Table('ads', metadata, sa.Column('id', sa.Integer, primary_key=True),
                     sa.Column('title', sa.String(100), nullable=False),
                     sa.Column('description', sa.Text, nullable=False),
                     sa.Column('date', sa.DateTime, default=datetime.datetime.utcnow),
                     sa.Column('author', sa.Integer, sa.ForeignKey('user.id')))

user_table = sa.Table('user', metadata, sa.Column('id', sa.Integer, primary_key=True),
                      sa.Column('username', sa.String(64), unique=True),
                      sa.Column('email', sa.String(120), unique=True),
                      sa.Column('password', sa.String(128)),
                      sa.Column('token', sa.String(500), unique=True))


def check_password(raw_password: str):
    raw_password = f'{raw_password}{SALT}'
    password = hashlib.md5(raw_password.encode()).hexdigest()
    return password


async def register_connection(app: web.Application):
    pg_pool = await pool.create_pool(SQLALCHEMY_DATABASE_URI)
    app['pg_pool'] = pg_pool
    yield
    pg_pool.close()


async def register_connection_alchemy(app: web.Application):
    engine = await create_engine(
        dsn=SQLALCHEMY_DATABASE_URI,
        minsize=2,
        maxsize=10
    )

    app['pg_engine'] = engine
    yield
    engine.close()


register_connection_callback = partial(register_connection)

app.cleanup_ctx.append(partial(register_connection_alchemy))

app.cleanup_ctx.append(partial(register_connection_callback))


@routes.post('/post')
async def create_user(request):
    """ Функция создания пользователя """
    post_data = await request.json()
    try:
        username = post_data['username']
        email = post_data['email']
        password = hashlib.md5(post_data['password'].encode()).hexdigest()
    except KeyError:
        raise web.HTTPBadRequest
    engine = request.app['pg_engine']

    async with engine.acquire() as conn:
        result = await conn.execute(user_table.insert().values(username=username, email=email, password=password))
        user = await result.fetchone()
        return web.json_response({'user_id': user[0]})


@routes.get('/{user_id}')
async def user_detail(request):
    """ Функция вывода информации о пользователе """
    user_id = request.match_info['user_id']
    pg_pool = request.app['pg_pool']
    async with pg_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # await cursor.execute(f'SELECT * FROM user WHERE id = {user_id};')
            await cursor.execute(user_table.select([user_table.c.id, user_table.c.username]).select_from(user_table).
                                 where(user_table.c.id == user_id))
            result = await cursor.fetchone()
            if result:
                return web.json_response(
                    {'id': result[0],
                     'username': result[1]}
                )
    raise web.HTTPNotFound()


@routes.get('/{user_id}')
async def user_del(request):
    """ Функция удаления пользователя """
    user_id = request.match_info['user_id']
    pg_pool = request.app['pg_pool']
    async with pg_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # await cursor.execute(user_table.delete(user_table).where(user_table.c.id == user_id))
            await cursor.execute(f'DELETE FROM user WHERE id = {user_id}')
    return f"Пользователь с id = {user_id} удалён"


@routes.post('/post')
async def login(request):
    """ Функция авторизации пользователя с присвоением токена """
    post_data = await request.json()
    try:
        username = post_data['username']
        password = hashlib.md5(post_data['password'].encode()).hexdigest()
        pg_pool = request.app['pg_pool']
        authorized = check_password(password)
        if not authorized:
            return web.json_response({'error': 'Password invalid'})
        expires = datetime.timedelta(days=7)
        async with pg_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # result = await cursor.execute(f'SELECT * FROM user WHERE username = {username};')
                result = await cursor.execute(user_table.select([user_table.c.id]).select_from(user_table).
                                              where(user_table.c.username == username))
                user = await result.fetchone()
                if result:
                    access_token = create_access_token(identity=str(result[0]), expires_delta=expires)
                    user.token = access_token
                    return web.json_response(
                        {'token': access_token})
    except KeyError:
        raise web.HTTPBadRequest


@routes.get('/{ad_id}')
async def ad_info(request):
    """ Функция просмотра объявлений по ID """
    ad_id = request.match_info['ad_id']
    pg_pool = request.app['pg_pool']
    async with pg_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # result = await cursor.execute(f'SELECT * FROM ads WHERE id = {ad_id};')
            result = await cursor.execute(ads_table.select().where(ads_table.c.id == ad_id))
            ad = await result.fetchone()
            if ad is None:
                return "Выбранный ID объявления не существует - проверьте правильность ввода"
            return web.json_response(
                {'ad_id': result[0],
                 'title': result[1],
                 'description': result[2],
                 'date': result[3],
                 'author': result[4]}
            )


@routes.post('/post')
async def create_ad(request):
    """ Функция создания объявления """
    token = await (dict(request.headers))['Authorization'].split(' ')[1]
    post_data = await request.json()
    pg_pool = request.app['pg_pool']
    async with pg_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            result = await cursor.execute(user_table.select()
                                          .where(user_table.c.token == token))
            authorized_user = await result.fetchone()
            if authorized_user is None:
                return "Токен не существует - проверьте правильность ввода"
            title = post_data['title']
            description = post_data['description']
            author = result[1]
            engine = request.app['pg_engine']
            async with engine.acquire() as connection:
                result = await connection.execute(
                    ads_table.insert().values(title=title, description=description, author=author))
                ad = await result.fetchone()
                return web.json_response({'ad_id': ad[0],
                                          'title': ad[1]})


@routes.post('/{ad_id}')
async def update_ad(request):
    """ Функция обновления объявления """
    post_data = await request.json()
    ad_id = request.match_info['ad_id']
    pg_pool = request.app['pg_pool']
    async with pg_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            result = await cursor.execute(ads_table.select()
                                          .where(ads_table.c.id == ad_id))
            ad_info1 = await result.fetchone()
            if ad_info1 is None:
                return "Выбранный ID объявления не существует - проверьте правильность ввода"
            token = await (dict(request.headers))['Authorization'].split(' ')[1]
            async with pg_pool.acquire() as connection:
                async with connection.cursor() as cursor1:
                    result = await cursor1.execute(user_table.select()
                                                   .where(user_table.c.token == token))
                    authorized_user = await result.fetchone()
                    if authorized_user is None:
                        return "Токен не существует - проверьте правильность ввода"
                    if authorized_user[0] == ad_info1[4]:
                        await conn.execute(ads_table.update(ads_table).values({'title': post_data[1],
                                                                               'description': post_data[2]})
                                           .where(ads_table.c.author == authorized_user[1]))
                    return web.json_response({'ad_id': ad_info1[0],
                                              'title': ad_info1[1],
                                              'description': ad_info1[2]})


@routes.get('/{ad_id}')
async def ad_del(request):
    """ Функция удаления объявления """
    ad_id = request.match_info['ad_id']
    pg_pool = request.app['pg_pool']
    async with pg_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            result = await cursor.execute(ads_table.select()
                                          .where(ads_table.c.id == ad_id))
            ad_info1 = await result.fetchone()
            if ad_info1 is None:
                return "Выбранный ID объявления не существует - проверьте правильность ввода"
            token = await (dict(request.headers))['Authorization'].split(' ')[1]
            async with pg_pool.acquire() as connection:
                async with connection.cursor() as cursor1:
                    result = await cursor1.execute(user_table.select()
                                                   .where(user_table.c.token == token))
                    authorized_user = await result.fetchone()
                    if authorized_user is None:
                        return "Токен не существует - проверьте правильность ввода"
                    async with pg_pool.acquire() as c:
                        async with c.cursor() as cursor2:
                            await cursor2.execute(user_table.delete(ads_table).where(ads_table.c.id == ad_id))
                    return 'Объявление удалено'


if __name__ == '__main__':
    app.add_routes([web.get(r'/api/v1/user-info/{user_id:\d+}', user_detail),
                    web.get(r'/api/v1/user-info/{user_id:\d+}/del', user_del),
                    web.post('/api/v1/user-create/', create_user),
                    web.post('/api/v1/auth/login', login),
                    web.get(r'/api/v1/ad-info/{ad_id:\d+}', ad_info),
                    web.post(r'/api/v1/ad-info/{ad_id:\d+}/update/', update_ad),
                    web.post('/api/v1/ad-create/', create_ad),
                    web.get(r'/api/v1/ad-info/{ad_id:\d+}/del', ad_del)])
    web.run_app(app, host='127.0.0.1', port=8080)
