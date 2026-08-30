import asyncio
import os
import functools
import update

from dotenv import load_dotenv
from datetime import timedelta

from hypercorn import Config
from hypercorn.asyncio import serve
from babel import Locale

from quart_babel import Babel
from quart import (
    Quart,
    render_template,
    redirect,
    url_for,
    jsonify,
    session,
    websocket,
    request
)

from objects import (
    Settings,
    UserPool,
    BotPool,
    User
)

from utils import (
    DISCORD_API_BASE_URL,
    ROOT_DIR,
    LANGUAGES,
    get_locale,
    requests_api,
    process_js_files,
    compile_scss,
    download_geoip_db,
    check_country_with_ip,
    check_version,
    setup_logging
)

SETTINGS: Settings = Settings()

app = Quart(__name__)
app.secret_key = SETTINGS.secret_key

babel = Babel(app)
babel.init_app(app, locale_selector=get_locale)

load_dotenv()

def login_required(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        token = session.get("discord_token", None)
        if not token:
            return redirect(url_for('login'))

        user = UserPool.get(token=token)
        if not user:
            resp = await requests_api(f'{DISCORD_API_BASE_URL}/users/@me', headers={'Authorization': f'Bearer {token}'})
            if resp:
                resp["access_token"] = token
                user = UserPool.add(resp)
            else:
                return redirect(url_for('login'))
            
        return await func(user, *args, **kwargs)
    return wrapper

@app.before_serving
async def setup():
    lang_codes = ["en"] + [
        lang for lang in os.listdir(os.path.join(ROOT_DIR, "translations"))
        if not lang.startswith(".")
    ]
    for lang_code in lang_codes:
        LANGUAGES[lang_code] = {"name": Locale.parse(lang_code).get_display_name(lang_code).capitalize()}

    process_js_files()
    compile_scss()
    await download_geoip_db()

@app.route("/health", methods=["GET"])
async def health():
    return jsonify({"status": "ok"}), 200

@app.route("/", methods=["GET"])
async def home():
    token = session.get("discord_token", None)
    if not token:
        return redirect(url_for('login'))
    
    user = UserPool.get(token=token)

    forwarded_for = request.headers.get('X-Forwarded-For')
    user_ip = forwarded_for.split(',')[0] if forwarded_for else request.remote_addr
    country = await check_country_with_ip(user_ip)

    if not user:
        resp = await requests_api(f'{DISCORD_API_BASE_URL}/users/@me', headers={'Authorization': f'Bearer {token}'})
        if resp:
            resp["access_token"] = token
            resp["country"] = country
            user = UserPool.add(resp)
        else:
            return redirect(url_for('login'))

    else:
        user.country = country

    return await render_template("index.html", user=user, languages=LANGUAGES)

@app.route("/login", methods=["GET"])
async def login():
    params = {
        'client_id': SETTINGS.client_id,
        'response_type': 'code',
        'redirect_uri': SETTINGS.redirect_url,
        'scope': 'identify+guilds'
    }
    return redirect(f'{DISCORD_API_BASE_URL}/oauth2/authorize?{"&".join([f"{k}={v}" for k, v in params.items()])}')

@app.route('/logout', methods=["GET"])
@login_required
async def logout(user: User):
    session.pop("discord_token", None)
    
    return redirect(url_for("home"))

@app.route('/callback')
async def callback():
    code = request.args.get('code')
    data = {
        'client_id': SETTINGS.client_id,
        'client_secret': SETTINGS.client_secret_id,
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': SETTINGS.redirect_url,
        'scope': 'identify'
    }
    token_data = await requests_api(f'{DISCORD_API_BASE_URL}/oauth2/token', 'POST', data=data)
    if token_data:
        session.permanent = True
        app.permanent_session_lifetime = timedelta(days=30)
        session['discord_token'] = token_data.get("access_token")

    return redirect(url_for("home"))

@app.route('/language/<language>')
@login_required
async def set_language(user: User, language = None):
    if language in LANGUAGES:
        session["language_code"] = language
    return redirect(url_for('home'))

@app.errorhandler(404)
async def not_found(error):
    return redirect(url_for("home"))


import aiohttp
import asyncio

import aiohttp
import asyncio

ipc_task_started = False

import aiohttp
import asyncio

ipc_task_started = False

async def connect_to_remote_bot():
    url = "ws://51.79.162.167:25587/"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=15.0) as ws:
                    class WSWrap:
                        def __init__(self, ws):
                            self.ws = ws
                            self.headers = {
                                "User-Id": "1515038282185052342",
                                "Client-Name": "Gana",
                                "Client-Avatar": ""
                            }
                        async def send_json(self, data):
                            await self.ws.send_json(data)
                        async def receive(self):
                            msg = await self.ws.receive()
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                raise Exception(f"WebSocket Closed: {msg.data}")
                            print(f"[IPC IN] {msg.data}", flush=True)
                            return msg.data
                        async def close(self, code):
                            await self.ws.close(code=code)
                            
                    print("[IPC] Connected to remote Node.js bot IPC!", flush=True)
                    await BotPool.create("1515038282185052342", WSWrap(ws))
                    
                    while not ws.closed:
                        await asyncio.sleep(1)
        except Exception as e:
            pass
            await asyncio.sleep(5)

@app.before_serving
async def startup():
    global ipc_task_started
    if not ipc_task_started:
        ipc_task_started = True
        asyncio.create_task(connect_to_remote_bot())


@app.websocket("/ws_user")
@login_required
async def ws_user(user: User):
    try:
        await user.connect(websocket._get_current_object())
    except asyncio.CancelledError:
        raise

if __name__ == "__main__":
    update.check_version(with_msg=True)
    setup_logging(SETTINGS.logging)
    config = Config()
    config.bind = [f"{SETTINGS.host}:{SETTINGS.port}"]
    asyncio.run(serve(app, config))

    # For Testing
    # app.run(host=SETTINGS.host, port=SETTINGS.port, debug=True)