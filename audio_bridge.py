import asyncio
import websockets
from websockets.http11 import Response
from websockets.datastructures import Headers
import os
import mimetypes
import pyaudio
import ggwave
import time
import math
import struct

# =====================================================================
# --- КОНФІГУРАЦІЯ СЕРВЕРА ТА ЛІНКА ---
# =====================================================================

HTML_FILE = "chess_mesh.html" # Гра, яка відкриється за замовчуванням
clients = set()               # Список підключених браузерів (вкладок)
loop = None                   # Глобальний асинхронний цикл

# --- НАЛАШТУВАННЯ GGWAVE (АКУСТИЧНИЙ ПРОТОКОЛ) ---
# 0 = AUDIBLE_NORMAL (Дуже повільно, макс. надійність для поганих рацій)
# 1 = AUDIBLE_FAST (Середня швидкість, ідеально для звичайного ефіру)
# 2 = AUDIBLE_FASTEST (Швидко, для прямого кабелю або близької відстані)
# 3, 4, 5 = ULTRASOUND (Ультразвук, працює ТІЛЬКИ без рацій, просто по кімнаті)
GGWAVE_PROTOCOL = 1 

# Прапорець напівдуплексу: коли ми передаємо дані, ми "закриваємо вуха", 
# щоб не зловити власне відлуння і не відправити дані по колу.
is_transmitting = False

# --- НАЛАШТУВАННЯ VOX (ДЛЯ АНАЛОГОВИХ РАЦІЙ) ---
VOX_ENABLED = True       # Увімкнути "прогрів" рації перед відправкою даних
VOX_TONE_FREQ = 1000     # Частота тону прогріву (1000 Гц пробивається найкраще)
VOX_DELAY_MS = 600       # Скільки мілісекунд пищати, щоб рація встигла увімкнути передачу

# --- НАЛАШТУВАННЯ АУДІО ---
SAMPLE_RATE = 48000      # Стандартна частота дискретизації
CHUNK_SIZE = 1024        # Розмір "шматка" аудіо, який ми забираємо з мікрофона

# Ініціалізація рушіїв звуку
ggwave_instance = ggwave.init()
pa = pyaudio.PyAudio()


# =====================================================================
# --- ВЕБ-СЕРВЕР (РОЗДАЧА ФАЙЛІВ ТА СТАТИКИ) ---
# =====================================================================
async def http_handler(request):
    """Віддає браузеру HTML-сторінки, картинки (img), скрипти (js) та стилі (css)"""
    path = request.path if request.path != "/" else f"/{HTML_FILE}"
    file_path = path.lstrip("/")
    
    # Якщо файл знайдено на диску - віддаємо його
    if os.path.exists(file_path) and os.path.isfile(file_path):
        with open(file_path, "rb") as f:
            content = f.read()
        mime_type, _ = mimetypes.guess_type(file_path)
        mime_type = mime_type or "application/octet-stream"
        headers = Headers([("Content-Type", mime_type)])
        return Response(200, "OK", headers, content)
    else:
        # Якщо браузер просить те, чого немає
        headers = Headers([("Content-Type", "text/plain")])
        return Response(404, "Not Found", headers, b"404 Not Found")


# =====================================================================
# --- ЛОГІКА МЕРЕЖІ ---
# =====================================================================
async def send_node_list(websocket):
    """Надсилає в браузер список доступних користувачів (заглушка для акустики)"""
    await websocket.send("NODES:Acoustic-Node-1")


def generate_vox_tone(freq, duration_ms, rate):
    """Генерує монотонний 'БІІІП' для активації VOX на рації"""
    num_samples = int(rate * (duration_ms / 1000.0))
    tone_data = bytearray()
    for i in range(num_samples):
        # 0.5 - це гучність (щоб не перевантажити мікрофон)
        sample = 0.5 * math.sin(2 * math.pi * freq * i / rate)
        tone_data.extend(struct.pack('f', sample))
    return bytes(tone_data)


# =====================================================================
# --- АУДІО ПРИЙОМНИК (СЛУХАЄ ЕФІР) ---
# =====================================================================
def audio_callback(in_data, frame_count, time_info, status):
    """Ця функція працює у фоні постійно і ловить звуки з мікрофона"""
    global is_transmitting
    
    # Якщо ми зараз пищимо у динамік - ігноруємо мікрофон (Half-Duplex)
    if is_transmitting:
        return (in_data, pyaudio.paContinue)

    # Намагаємося розшифрувати звук через ggwave
    res = ggwave.decode(ggwave_instance, in_data)
    if res:
        try:
            # Якщо ggwave знайшов корисний текст - декодуємо його
            text = res.decode('utf-8')
            print(f"[🎤 МІКРОФОН -> ГРА]: {text}")
            
            # Перекидаємо отриманий текст усім підключеним браузерам
            if loop:
                for ws in list(clients):
                    asyncio.run_coroutine_threadsafe(ws.send(text), loop)
        except Exception:
            pass # Ігноруємо цифрове сміття
            
    return (in_data, pyaudio.paContinue)


# =====================================================================
# --- АУДІО ПЕРЕДАВАЧ (ПИЩИТЬ В ЕФІР) ---
# =====================================================================
def transmit_audio(text):
    """Перетворює текст гри на звук і відправляє в динаміки"""
    global is_transmitting
    
    # Блокуємо мікрофон, щоб не чути самих себе
    is_transmitting = True
    print(f"[ГРА -> 🔊 ДИНАМІК]: {text}")
    
    try:
        # Кодуємо текст в аудіотрель ggwave з нашим GGWAVE_PROTOCOL
        waveform = ggwave.encode(
            text, 
            protocolId=GGWAVE_PROTOCOL, 
            volume=50, 
            instance=ggwave_instance
        )
        
        # Відкриваємо динаміки для відтворення
        out_stream = pa.open(format=pyaudio.paFloat32,
                             channels=1,
                             rate=SAMPLE_RATE,
                             output=True)
                             
        # 1. Пищимо монотонним звуком (відкриваємо VOX рації)
        if VOX_ENABLED:
            vox_tone = generate_vox_tone(VOX_TONE_FREQ, VOX_DELAY_MS, SAMPLE_RATE)
            out_stream.write(vox_tone)
            
        # 2. Відразу ж пищимо корисні дані (наш пакет ggwave)
        out_stream.write(waveform)
        
        # Закриваємо динаміки
        out_stream.stop_stream()
        out_stream.close()
        
        # Чекаємо півсекунди, щоб відлуння в кімнаті повністю розсіялось
        time.sleep(0.5)
    finally:
        # Знову дозволяємо мікрофону слухати
        is_transmitting = False


# =====================================================================
# --- WEBSOCKET СЕРВЕР (ЗВ'ЯЗОК З БРАУЗЕРОМ) ---
# =====================================================================
async def ws_handler(websocket):
    """Отримує кліки/повідомлення з браузера і передає їх на динамік"""
    clients.add(websocket)
    await send_node_list(websocket)
    try:
        async for message in websocket:
            if message == "GET_NODES":
                await send_node_list(websocket)
                continue
            
            # Якщо повідомлення має правильний формат (Кому|Що)
            if "|" in message:
                target_nick, payload = message.split("|", 1)
                # Відправляємо на передачу в окремому потоці (щоб не блокувати сервер)
                loop.run_in_executor(None, transmit_audio, payload)
    finally:
        clients.remove(websocket)


async def router(connection, request):
    """Сортує запити: WebSocket йдуть у ws_handler, а файли - в http_handler"""
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None 
    return await http_handler(request)


# =====================================================================
# --- ГОЛОВНИЙ ЦИКЛ ЗАПУСКУ ---
# =====================================================================
async def main():
    global loop
    loop = asyncio.get_running_loop()
    
    print("🎤 Ініціалізація акустичного модема (VOX + Half-Duplex)...")
    
    try:
        # Відкриваємо мікрофон у фоновому режимі (він викликає audio_callback постійно)
        in_stream = pa.open(format=pyaudio.paFloat32,
                            channels=1,
                            rate=SAMPLE_RATE,
                            input=True,
                            frames_per_buffer=CHUNK_SIZE,
                            stream_callback=audio_callback)
        in_stream.start_stream()
        print(f"✅ Мікрофон активний. Режим ggwave: {GGWAVE_PROTOCOL}. Слухаю ефір...")
    except Exception as e:
        print(f"❌ Помилка доступу до мікрофона: {e}")
        return

    print("🚀 Сервер запущено: http://127.0.0.1:8890")
    
    # Запуск сервера
    async with websockets.serve(ws_handler, "0.0.0.0", 8890, process_request=router):
        await asyncio.Future() 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Зупинка сервера...")
    finally:
        # При виході коректно звільняємо аудіоресурси
        pa.terminate()
        ggwave.free(ggwave_instance)
