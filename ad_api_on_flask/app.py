import hashlib
import datetime
import os
from functools import partial
import jwt
from aiohttp_jwt import JWTMiddleware, login_required
from aiohttp import web
from aiopg import pool
from aiopg.sa import create_engine
import sqlalchemy as sa

SALT = 'my_salt'

jwt_middleware = JWTMiddleware(
    SALT, request_property="id", credentials_required=False
)
routes = web.RouteTableDef()
app = web.Application(middlewares=[jwt_middleware])
SQLALCHEMY_DATABASE_URI = 'postgresql://{}:{}@localhost:5432/ad_api'.format(os.getenv('DB_USER'),
                                                                            os.getenv('DB_PASSWORD'))
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


async def get_token(username):
    return jwt.encode({"username": username}, SALT)


def check_password(raw_password: str):
    raw_password = f'{raw_password}{SALT}'
    password = hashlib.md5(raw_password.encode()).hexdigest()
    return password


async def register_connection_alchemy(app: web.Application):
    engine = await create_engine(
        dsn=SQLALCHEMY_DATABASE_URI,
        minsize=2,
        maxsize=10
    )

    app['pg_engine'] = engine
    yield
    engine.close()

app.cleanup_ctx.append(partial(register_connection_alchemy))


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
    engine = request.app['pg_engine']
    async with engine.acquire() as conn:
        async for i in conn.execute(user_table.select()):
            if i.id == int(user_id):
                return web.json_response(
                    {'id': i.id,
                     'username': i.username}
                )
        return web.json_response(
            {user_id: "No such user_id"}
        )


@routes.get('/{user_id}')
async def user_del(request):
    """ Функция удаления пользователя """
    user_id = request.match_info['user_id']
    engine = request.app['pg_engine']
    async with engine.acquire() as conn:
        async for i in conn.execute(user_table.select()):
            if i.id == int(user_id):
                await conn.execute(user_table.delete().where(user_table.c.id == int(user_id)))
                return web.json_response(
                    {user_id: "User deleted"}
                )
        return web.json_response(
            {user_id: "No such user_id"}
        )


@routes.post('/post')
async def login(request):
    """ Функция авторизации пользователя с присвоением токена """
    post_data = await request.json()
    try:
        username = post_data['username']
        password = hashlib.md5(post_data['password'].encode()).hexdigest()
        engine = request.app['pg_engine']
        authorized = check_password(password)
        if not authorized:
            return web.json_response({'error': 'Password invalid'})
        async with engine.acquire() as conn:
            await conn.execute(user_table.select().where(user_table.c.username == username))
            access_token = await get_token(username)
            await conn.execute(sa.update(user_table).values({'token': access_token})
                               .where(user_table.c.username == username))
            return web.json_response(
                {'token': access_token})
    except KeyError:
        raise web.HTTPBadRequest


@routes.get('/{ad_id}')
async def ad_info(request):
    """ Функция просмотра объявлений по ID """
    ad_id = request.match_info['ad_id']
    engine = request.app['pg_engine']
    async with engine.acquire() as conn:
        result = await conn.execute(ads_table.select().where(ads_table.c.id == int(ad_id)))
        ad = await result.fetchone()
        if ad is None:
            return web.json_response({"id": "Выбранный ID объявления не существует - проверьте правильность ввода"})
        return web.json_response(
            {'ad_id': ad[0],
             'title': ad[1],
             'description': ad[2],
             'date': str(ad[3]),
             'author': ad[4]}
        )


@login_required
@routes.post('/post')
async def create_ad(request):
    """ Функция создания объявления """
    token = request.headers['Authorization'].split()[1]
    post_data = await request.json()
    engine = request.app['pg_engine']
    async with engine.acquire() as conn:
        result = await conn.execute(user_table.select()
                                    .where(user_table.c.token == token))
        authorized_user = await result.fetchone()
        if authorized_user is None:
            return web.json_response({"token": "Токен не существует - проверьте правильность ввода"})
        title = post_data['title']
        description = post_data['description']
        author = authorized_user[0]
        engine = request.app['pg_engine']
        async with engine.acquire() as connection:
            result = await connection.execute(
                ads_table.insert().values(title=title, description=description, author=author))
            ad = await result.fetchone()
            print(ad)
            return web.json_response({'Объявление создано. ID': ad[0]})


@login_required
@routes.post('/{ad_id}')
async def update_ad(request):
    """ Функция обновления объявления """
    ad_id = request.match_info['ad_id']
    engine = request.app['pg_engine']
    token = request.headers['Authorization'].split()[1]
    async with engine.acquire() as conn:
        result = await conn.execute(ads_table.select()
                                    .where(ads_table.c.id == int(ad_id)))
        ad_info_for_upd = await result.fetchone()
        if ad_info_for_upd is None:
            return web.json_response({"id": "Выбранный ID объявления не существует - проверьте правильность ввода"})
        result = await conn.execute(user_table.select()
                                    .where(user_table.c.token == token))
        authorized_user = await result.fetchone()
        if authorized_user is None:
            return web.json_response({"Token": "Пользователь не авторизован"})
        post_data = await request.json()
        await conn.execute(sa.update(ads_table).values({'title': post_data['title'],
                                                        'description': post_data['description']})
                           .where(ads_table.c.id == int(ad_id)))
        return web.json_response({'ad_info': 'Данные обновлены',
                                  'title': post_data['title'],
                                  'description': post_data['description']})


@login_required
@routes.get('/{ad_id}')
async def ad_del(request):
    """ Функция удаления объявления """
    ad_id = request.match_info['ad_id']
    engine = request.app['pg_engine']
    async with engine.acquire() as conn:
        result = await conn.execute(ads_table.select()
                                    .where(ads_table.c.id == int(ad_id)))
        ad_info1 = await result.fetchone()
        if ad_info1 is None:
            return web.json_response({"ID": "Выбранный ID объявления не существует - проверьте правильность ввода"})
        token = request.headers['Authorization'].split()[1]
        result = await conn.execute(user_table.select()
                                    .where(user_table.c.token == token))
        authorized_user = await result.fetchone()
        if authorized_user is None:
            return web.json_response({"token": "Токен не существует - проверьте правильность ввода"})
        await conn.execute(ads_table.delete().where(ads_table.c.id == int(ad_id)))
        return web.json_response({'message': 'Объявление удалено'})


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
