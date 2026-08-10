"""
Сервер мини-аппа настроек городов.

Что делает:
- Отдаёт страницу index.html и locations.json
- Проверяет, что запрос реально пришёл из Telegram (initData), а не от
  кого попало, кто угадал адрес
- Хранит список выключенных городов/районов в Upstash Redis (общее
  хранилище, доступное и отсюда, и из parser_turkey.py)

Переменные окружения (задаются на хостинге, не в коде):
  BOT_TOKEN            — тот же токен бота, что и в parser_turkey.py
  UPSTASH_REDIS_URL    — REST-адрес базы Upstash
  UPSTASH_REDIS_TOKEN  — токен доступа Upstash
"""
import hashlib
import hmac
import json
import os
import time
from urllib.parse import parse_qsl

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

BOT_TOKEN = os.environ["BOT_TOKEN"]
UPSTASH_URL = os.environ["UPSTASH_REDIS_URL"].rstrip("/")
UPSTASH_TOKEN = os.environ["UPSTASH_REDIS_TOKEN"]
REDIS_KEY = "disabled_locations"
REQUIRE_PRICE_KEY = "require_price"
RENTAL_PERIOD_FILTER_KEY = "rental_period_filter"
# Сколько секунд считаем initData ещё свежим (защита от повторной отправки
# перехваченного старого запроса) — Telegram сам обновляет initData при
# каждом открытии мини-аппа, так что запас в сутки более чем достаточен
MAX_INIT_DATA_AGE_SECONDS = 24 * 60 * 60

app = FastAPI()


def verify_init_data(init_data: str) -> dict:
    """Проверяет подпись initData по алгоритму Telegram. Возвращает
    распарсенные данные, если подпись верна, иначе бросает исключение.
    https://core.telegram.org/bots/webapps#validating-data-received-via-the-web-app
    """
    if not init_data:
        raise HTTPException(401, "Нет initData — открой это через кнопку в боте")

    parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        raise HTTPException(401, "initData без подписи")

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        raise HTTPException(401, "Неверная подпись — открой мини-апп заново через бота")

    auth_date = int(parsed.get("auth_date", "0"))
    if time.time() - auth_date > MAX_INIT_DATA_AGE_SECONDS:
        raise HTTPException(401, "Сессия устарела — открой мини-апп заново через бота")

    return parsed


async def redis_get_disabled():
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{UPSTASH_URL}/smembers/{REDIS_KEY}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
        )
        resp.raise_for_status()
        return resp.json().get("result", [])


async def redis_set_disabled(names):
    async with httpx.AsyncClient() as client:
        # Заменяем содержимое множества целиком: удаляем старое, кладём новое
        await client.post(
            f"{UPSTASH_URL}/pipeline",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=[["DEL", REDIS_KEY]] + ([["SADD", REDIS_KEY] + names] if names else [])
        )


async def redis_get_require_price():
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{UPSTASH_URL}/get/{REQUIRE_PRICE_KEY}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
        )
        resp.raise_for_status()
        val = resp.json().get("result")
        return val == "1"


async def redis_set_require_price(value: bool):
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{UPSTASH_URL}/set/{REQUIRE_PRICE_KEY}/{1 if value else 0}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
        )


async def redis_get_rental_period_filter():
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{UPSTASH_URL}/get/{RENTAL_PERIOD_FILTER_KEY}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
        )
        resp.raise_for_status()
        val = resp.json().get("result")
        return val if val in ("all", "monthly", "daily") else "all"


async def redis_set_rental_period_filter(value: str):
    if value not in ("all", "monthly", "daily"):
        raise HTTPException(400, "rentalPeriodFilter должен быть 'all', 'monthly' или 'daily'")
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{UPSTASH_URL}/set/{RENTAL_PERIOD_FILTER_KEY}/{value}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
        )


PRICE_RANGE_KEYS = ("monthly_price_min", "monthly_price_max", "daily_price_min", "daily_price_max")


async def redis_get_price_ranges():
    result = {}
    async with httpx.AsyncClient() as client:
        for key in PRICE_RANGE_KEYS:
            resp = await client.get(
                f"{UPSTASH_URL}/get/{key}",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
            )
            resp.raise_for_status()
            val = resp.json().get("result")
            result[key] = int(val) if val not in (None, "", "none") else None
    return result


async def redis_set_price_ranges(values: dict):
    async with httpx.AsyncClient() as client:
        for key in PRICE_RANGE_KEYS:
            if key not in values:
                continue
            v = values[key]
            if v is not None and not isinstance(v, int):
                raise HTTPException(400, f"{key} должен быть числом или null")
            await client.post(
                f"{UPSTASH_URL}/set/{key}/{v if v is not None else 'none'}",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
            )


@app.get("/api/state")
async def get_state(initData: str = ""):
    verify_init_data(initData)
    disabled = await redis_get_disabled()
    require_price = await redis_get_require_price()
    rental_period_filter = await redis_get_rental_period_filter()
    price_ranges = await redis_get_price_ranges()
    return JSONResponse({
        "disabled": disabled,
        "requirePrice": require_price,
        "rentalPeriodFilter": rental_period_filter,
        "priceRanges": price_ranges,
    })


@app.post("/api/state")
async def save_state(request: Request):
    body = await request.json()
    verify_init_data(body.get("initData", ""))
    names = body.get("disabled", [])
    if not isinstance(names, list):
        raise HTTPException(400, "disabled должен быть списком")
    await redis_set_disabled([str(n) for n in names])
    if "requirePrice" in body:
        await redis_set_require_price(bool(body["requirePrice"]))
    if "rentalPeriodFilter" in body:
        await redis_set_rental_period_filter(str(body["rentalPeriodFilter"]))
    if "priceRanges" in body and isinstance(body["priceRanges"], dict):
        await redis_set_price_ranges(body["priceRanges"])
    return JSONResponse({"ok": True})


# Отдаём саму страницу и JSON со списком локаций как статику
app.mount("/", StaticFiles(directory="static", html=True), name="static")
