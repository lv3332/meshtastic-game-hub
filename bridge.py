import asyncio
import websockets
from websockets.http11 import Response
from websockets.datastructures import Headers
import meshtastic.serial_interface
from pubsub import pub
import os
import mimetypes

# --- КОНФІГУРАЦІЯ ---
# Файл, який відкриється при вході на http://127.0.0.1:8890
HTML_FILE = "chess_mesh.html"
clients = set()
interface = None
loop = None  

# --- HTTP ХЕНДЛЕР (Роздача статичних файлів) ---
async def http_handler(request):
    path = request.path if request.path != "/" else f"/{HTML_FILE}"
    file_path = path.lstrip("/")
    
    # Перевіряємо наявність файлу
    if os.path.exists(file_path) and os.path.isfile(file_path):
        with open(file_path, "rb") as f:
            content = f.read()
            
        # Автоматичне визначення MIME-типу (html, js, css, png)
        mime_type, _ = mimetypes.guess_type(file_path)
        mime_type = mime_type or "application/octet-stream"
        
        headers = Headers([("Content-Type", mime_type)])
        return Response(200, "OK", headers, content)
    else:
        headers = Headers([("Content-Type", "text/plain")])
        return Response(404, "Not Found", headers, b"404 Not Found")

# --- РОБОТА З НОДАМИ (Список онлайн) ---
async def send_node_list(websocket):
    if interface and interface.nodes:
        nicks = [node.get('user', {}).get('shortName', '') 
                 for node in interface.nodes.values() 
                 if node.get('user', {}).get('shortName')]
        await websocket.send(f"NODES:{','.join(nicks)}")

# --- WEBSOCKET ХЕНДЛЕР (Міст між браузером і рацією) ---
async def ws_handler(websocket):
    clients.add(websocket)
    # При підключенні відразу віддаємо список нод
    await send_node_list(websocket)
    
    try:
        async for message in websocket:
            # Запит на оновлення списку нод
            if message == "GET_NODES":
                await send_node_list(websocket)
                continue
            
            # Пересилка повідомлень у радіоефір
            if "|" in message:
                target_nick, payload = message.split("|", 1)
                
                # Відправка Broadcast або Direct
                if target_nick == "BROADCAST" or target_nick == "":
                    interface.sendText(payload)
                else:
                    dest_id = None
                    for node in interface.nodes.values():
                        if node.get('user', {}).get('shortName') == target_nick:
                            dest_id = node.get('user', {}).get('id')
                            break
                    
                    if dest_id:
                        interface.sendText(payload, destinationId=dest_id)
                    else:
                        await websocket.send(f"SYS:ERROR:Користувача {target_nick} не знайдено.")
    finally:
        clients.remove(websocket)

# --- РОУТИНГ ---
async def router(connection, request):
    # Якщо це WebSocket-запит - повертаємо None (перехоплюється ws_handler)
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None 
    # В іншому випадку - віддаємо статику (HTML/JS/CSS)
    return await http_handler(request)

# --- КОЛБЕК MESHTASTIC (Отримання з ефіру) ---
def on_receive(packet, interface):
    decoded = packet.get('decoded', {})
    if 'text' in decoded:
        text = decoded['text']
        
        # Фільтр для всіх наших протоколів
        if text.startswith(("MB:", "CHESS:", "CHECKERS:", "CHAT:")):
            sender_id = packet.get('fromId')
            sender_nick = "Unknown"
            
            # Додаємо нікнейм для чату
            if interface.nodes and sender_id in interface.nodes:
                sender_nick = interface.nodes[sender_id].get('user', {}).get('shortName', sender_id)
            
            formatted_text = text
            if text.startswith("CHAT:"):
                raw_text = text.split(":", 1)[1]
                formatted_text = f"CHAT:{sender_nick}:{raw_text}"
            
            # Безпечна відправка в асинхронний цикл
            if loop:
                for ws in list(clients):
                    asyncio.run_coroutine_threadsafe(ws.send(formatted_text), loop)

# --- ГОЛОВНИЙ ЦИКЛ ---
async def main():
    global interface, loop
    loop = asyncio.get_running_loop()
    
    print("🔌 Ініціалізація Meshtastic...")
    try:
        interface = meshtastic.serial_interface.SerialInterface()
        pub.subscribe(on_receive, "meshtastic.receive")
        print("✅ Модем підключено!")
    except Exception as e:
        print(f"❌ Помилка: {e}")
        return

    print("🚀 Сервер запущено: http://127.0.0.1:8890")
    
    # Запуск сервера
    async with websockets.serve(ws_handler, "0.0.0.0", 8890, process_request=router):
        await asyncio.Future() 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Зупинка...")
    finally:
        if interface:
            interface.close()
