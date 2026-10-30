import os
import io
import re
import time
import random
import asyncio
import threading
import http.cookiejar
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp

try:
    import orjson
    class FastJSONResponse(Response):
        media_type = "application/json"
        def render(self, content: Any) -> bytes:
            return orjson.dumps(content)
except ImportError:
    FastJSONResponse = JSONResponse

# ==========================================
# 1. إعداد التطبيق والهندسة الفائقة
# ==========================================
app = FastAPI(
    title="Apex YouTube Hyper API",
    version="5.1.0",
    default_response_class=FastJSONResponse,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
)

EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="apex_hyper")
CONCURRENCY_GUARD = asyncio.Semaphore(50)
THREAD_LOCAL = threading.local()
IN_FLIGHT_REQUESTS: Dict[str, asyncio.Future] = {}

# ==========================================
# 2. إدارة الكوكيز في الذاكرة
# ==========================================
PREBUILT_COOKIE_JARS: List[http.cookiejar.MozillaCookieJar] = []

def _init_cookie_jars() -> None:
    raw_env = os.environ.get("YT_COOKIES", "").strip()
    if not raw_env:
        return
    blocks = [b.strip() for b in raw_env.split("===SPLIT===") if b.strip()]
    for block in blocks:
        if not block.startswith("# Netscape HTTP Cookie File"):
            block = "# Netscape HTTP Cookie File\n" + block
        jar = http.cookiejar.MozillaCookieJar()
        try:
            jar._really_load(
                io.StringIO(block),
                "memory_cookies.txt",
                ignore_discard=True,
                ignore_expires=True,
            )
            PREBUILT_COOKIE_JARS.append(jar)
        except Exception:
            continue

_init_cookie_jars()

# ==========================================
# 3. محرك الاستخراج والفرز
# ==========================================
YT_URL_ID_PATTERN = re.compile(
    r"(?:v=|\/v\/|youtu\.be\/|\/embed\/|\/shorts\/|\/live\/|^)([a-zA-Z0-9_-]{11})(?:[?&\/]|$)"
)

def _fast_resolve_target(raw_input: str) -> str:
    text = raw_input.strip()
    match = YT_URL_ID_PATTERN.search(text)
    if match:
        return f"https://www.youtube.com/watch?v={match.group(1)}"
    return text

def _parse_formats_fast(formats: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    audio_list = []
    combined_list = []
    ultra_video_list = []

    for f in formats:
        url = f.get("url")
        if not url:
            continue
        protocol = f.get("protocol", "")
        if "m3u8" in protocol or "dash_segments" in protocol:
            continue

        vcodec = f.get("vcodec") or "none"
        acodec = f.get("acodec") or "none"
        filesize = f.get("filesize") or f.get("filesize_approx") or 0
        size_mb = round(filesize / 1048576, 2) if filesize else None

        if acodec != "none" and vcodec == "none":
            abr = round(f.get("abr") or f.get("tbr") or 0)
            audio_list.append({
                "format_id": f.get("format_id"),
                "ext": f.get("ext"),
                "acodec": acodec,
                "bitrate_kbps": abr,
                "sample_rate": f.get("asr"),
                "filesize": filesize,
                "filesize_mb": size_mb,
                "url": url,
            })
        elif vcodec != "none" and acodec != "none":
            height = f.get("height") or 0
            fps = f.get("fps") or 30
            tbr = round(f.get("tbr") or 0)
            combined_list.append({
                "format_id": f.get("format_id"),
                "quality": f.get("format_note") or f"{height}p",
                "height": height,
                "width": f.get("width") or 0,
                "fps": fps,
                "ext": f.get("ext"),
                "vcodec": vcodec,
                "acodec": acodec,
                "bitrate_kbps": tbr,
                "filesize": filesize,
                "filesize_mb": size_mb,
                "url": url,
            })
        elif vcodec != "none" and acodec == "none":
            height = f.get("height") or 0
            fps = f.get("fps") or 30
            tbr = round(f.get("tbr") or 0)
            ultra_video_list.append({
                "format_id": f.get("format_id"),
                "quality": f.get("format_note") or f"{height}p",
                "height": height,
                "width": f.get("width") or 0,
                "fps": fps,
                "ext": f.get("ext"),
                "vcodec": vcodec,
                "bitrate_kbps": tbr,
                "filesize": filesize,
                "filesize_mb": size_mb,
                "url": url,
            })

    audio_list.sort(key=lambda x: (1 if x["ext"] == "m4a" else 0, x["bitrate_kbps"]), reverse=True)
    combined_list.sort(key=lambda x: (x["height"], x["fps"], x["bitrate_kbps"]), reverse=True)
    ultra_video_list.sort(key=lambda x: (x["height"], x["fps"], x["bitrate_kbps"]), reverse=True)

    return {
        "audio": audio_list,
        "combined": combined_list,
        "ultra": ultra_video_list,
    }

SINGLE_CLIENTS = [["android_vr"], ["ios"], ["tv"]]

def _get_hyper_ydl(client_idx: int) -> yt_dlp.YoutubeDL:
    engines = getattr(THREAD_LOCAL, "ydl_engines", None)
    if engines is None:
        engines = {}
        THREAD_LOCAL.ydl_engines = engines

    if client_idx not in engines:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
            "geo_bypass": True,
            "nocheckcertificate": True,
            "cachedir": False,
            "socket_timeout": 2.5,
            "retries": 0,
            "extractor_retries": 0,
            "check_formats": False,
            "youtube_include_dash_manifest": False,
            "youtube_include_hls_manifest": False,
            "default_search": "ytsearch1",
            "extractor_args": {
                "youtube": {
                    "player_client": SINGLE_CLIENTS[client_idx],
                    "player_skip": ["webpage", "configs"],
                    "skip": ["hls", "dash", "translated_subs"],
                }
            },
        }
        engines[client_idx] = yt_dlp.YoutubeDL(ydl_opts)

    ydl = engines[client_idx]
    if PREBUILT_COOKIE_JARS:
        chosen_jar = random.choice(PREBUILT_COOKIE_JARS)
        ydl.cookiejar.clear()
        for cookie in chosen_jar:
            ydl.cookiejar.set_cookie(cookie)
    return ydl

def _extract_with_client(target: str, mode: str, client_idx: int) -> Dict[str, Any]:
    ydl = _get_hyper_ydl(client_idx)
    info = ydl.extract_info(target, download=False)

    if "entries" in info:
        entries = [e for e in info["entries"] if e]
        if not entries:
            raise ValueError("لا توجد نتائج.")
        info = entries[0]

    parsed = _parse_formats_fast(info.get("formats", []))
    if not (parsed["audio"] or parsed["combined"] or parsed["ultra"]):
        raise ValueError("لا توجد صيغ متاحة.")

    best_audio = parsed["audio"][0] if parsed["audio"] else None
    best_combined = parsed["combined"][0] if parsed["combined"] else None
    best_ultra = parsed["ultra"][0] if parsed["ultra"] else None

    payload: Dict[str, Any] = {
        "status": True,
        "client_used": SINGLE_CLIENTS[client_idx][0],
        "metadata": {
            "id": info.get("id"),
            "title": info.get("title"),
            "channel": info.get("uploader"),
            "duration_sec": info.get("duration"),
            "views": info.get("view_count"),
            "thumbnail": info.get("thumbnail"),
            "webpage_url": info.get("webpage_url"),
        },
    }

    if mode == "audio":
        payload["direct_url"] = best_audio["url"] if best_audio else None
        payload["best_audio"] = best_audio
        payload["audio_formats"] = parsed["audio"]
    elif mode == "video":
        payload["direct_url"] = (
            best_combined["url"] if best_combined
            else (best_ultra["url"] if best_ultra else None)
        )
        payload["best_combined_video"] = best_combined
        payload["best_ultra_video"] = best_ultra
        payload["best_audio_for_merge"] = best_audio
        payload["combined_formats"] = parsed["combined"]
        payload["ultra_formats"] = parsed["ultra"]
    else:
        payload["best_audio"] = best_audio
        payload["best_combined_video"] = best_combined
        payload["best_ultra_video"] = best_ultra
        payload["formats"] = parsed

    return payload

async def _race_extraction(raw_target: str, mode: str) -> Dict[str, Any]:
    target = _fast_resolve_target(raw_target)
    loop = asyncio.get_running_loop()

    primary_task = loop.run_in_executor(EXECUTOR, _extract_with_client, target, mode, 0)
    try:
        return await asyncio.wait_for(asyncio.shield(primary_task), timeout=1.2)
    except (asyncio.TimeoutError, Exception):
        tasks = [
            loop.run_in_executor(EXECUTOR, _extract_with_client, target, mode, 1),
            loop.run_in_executor(EXECUTOR, _extract_with_client, target, mode, 2),
        ]
        if not primary_task.done():
            tasks.insert(0, primary_task)

        last_err = None
        for coro in asyncio.as_completed(tasks):
            try:
                return await coro
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"فشل الجلب السريع: {str(last_err)}")

# ==========================================
# 4. نقطة النهاية الرئيسية وصفحة الترحيب
# ==========================================
@app.get("/")
@app.get("/api")
@app.get("/api/extract")
async def main_extractor(
    response: Response,
    url: Optional[str] = Query(None, description="رابط يوتيوب أو الـ ID"),
    q: Optional[str] = Query(None, description="بحث بالاسم"),
    type: str = Query("all", pattern="^(audio|video|all)$"),
    redirect: bool = Query(False, description="توجيه مباشر للملف الخام"),
):
    target = (url or q or "").strip()
    
    # إذا دخل المستخدم على الرابط بدون معاملات، نعرض صفحة ترحيبية فاخرة
    if not target:
        html_content = """
        <!DOCTYPE html>
        <html lang="ar" dir="rtl">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Apex YouTube API</title>
            <style>
                body {
                    background-color: #0f172a;
                    color: #f8fafc;
                    font-family: system-ui, -apple-system, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                    margin: 0;
                }
                .card {
                    background: #1e293b;
                    padding: 30px;
                    border-radius: 16px;
                    box-shadow: 0 10px 25px rgba(0,0,0,0.3);
                    max-width: 550px;
                    width: 90%;
                    border: 1px solid #334155;
                }
                h1 { color: #38bdf8; margin-top: 0; font-size: 24px; text-align: center; }
                .info { background: #0f172a; padding: 15px; border-radius: 8px; margin: 15px 0; border-right: 4px solid #38bdf8; }
                .info p { margin: 5px 0; font-size: 15px; }
                .badge { color: #38bdf8; font-weight: bold; }
                h3 { color: #cbd5e1; font-size: 18px; margin-top: 20px; }
                ul { padding-right: 20px; color: #94a3b8; font-size: 14px; line-height: 1.8; }
                code { background: #0f172a; color: #38bdf8; padding: 2px 6px; border-radius: 4px; font-family: monospace; }
                .footer { text-align: center; margin-top: 25px; color: #64748b; font-size: 13px; }
            </style>
        </head>
        <body>
            <div class="card">
                <h1>⚡ Apex YouTube Hyper API</h1>
                <div class="info">
                    <p>👨‍💻 المطور: <span class="badge">@SHBH_S1</span></p>
                    <p>📢 القناة الرسمية: <span class="badge">@BOTATKILWA</span></p>
                </div>
                <h3>📖 طريقة الاستخدام المبسطة:</h3>
                <ul>
                    <li>جلب بيانات وروابط الفيديو بصيغة JSON:
                        <br><code>/?url=رابط_الفيديو&type=video</code>
                    </li>
                    <li>جلب روابط وأعلى جودة للصوت فقط:
                        <br><code>/?url=رابط_الفيديو&type=audio</code>
                    </li>
                    <li>البحث المباشر باسم الأغنية أو المقطع:
                        <br><code>/?q=اسم_البحث&type=all</code>
                    </li>
                    <li>التوجيه المباشر والتحميل السريع للملف الخام:
                        <br><code>/?url=رابط_الفيديو&type=audio&redirect=true</code>
                    </li>
                </ul>
                <div class="footer">🚀 يعمل بأعلى سرعة وأداء فائق على خوادم Vercel</div>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)

    start_t = time.perf_counter()
    flight_key = f"{_fast_resolve_target(target)}:{type}"

    try:
        if flight_key in IN_FLIGHT_REQUESTS:
            base_data = await IN_FLIGHT_REQUESTS[flight_key]
            data = dict(base_data)
        else:
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            IN_FLIGHT_REQUESTS[flight_key] = fut
            try:
                async with CONCURRENCY_GUARD:
                    base_data = await asyncio.wait_for(_race_extraction(target, type), timeout=6.5)
                fut.set_result(base_data)
                data = dict(base_data)
            except Exception as ex:
                if not fut.done():
                    fut.set_exception(ex)
                raise
            finally:
                IN_FLIGHT_REQUESTS.pop(flight_key, None)

        response.headers["Cache-Control"] = "public, s-maxage=7200, stale-while-revalidate=1800"
        data["process_time_ms"] = round((time.perf_counter() - start_t) * 1000, 1)

        if redirect:
            stream_url = (
                data.get("direct_url")
                or (data.get("best_audio") or {}).get("url")
                or (data.get("best_combined_video") or {}).get("url")
            )
            if stream_url:
                return RedirectResponse(url=stream_url, status_code=302)

        return data

    except Exception as e:
        return FastJSONResponse(
            status_code=500,
            content={
                "status": False,
                "error": str(e),
                "process_time_ms": round((time.perf_counter() - start_t) * 1000, 1),
            },
        )
