import asyncio
import websockets
from websockets.http11 import Response
from websockets.datastructures import Headers
import meshtastic.serial_interface
from pubsub import pub
import os
import mimetypes

# Файл за замовчуванням при переході на корінь http://127.0.0.1:8890
HTML_FILE = "chess_mesh.html"
clients = set()
interface = None
loop = None  

# Універсальний HTTP хендлер для статики (HTML, JS, CSS, PNG)
async def http_handler(request):
    path = request.path if request.path != "/" else f"/{HTML_FILE}"
    file_path = path.lstrip("/")
    
    # Перевіряємо, чи файл існує на диску і чи це дійсно файл, а не папка
    if os.path.exists(file_path) and os.path.isfile(file_path):
        with open(file_path, "rb") as f:
            content = f.read()
            
        # Автоматично визначаємо MIME-тип файлу для браузера
        mime_type, _ = mimetypes.guess_type(file_path)
        mime_type = mime_type or "application/octet-stream"
        
        headers = Headers([("Content-Type", mime_type)])
        return Response(200, "OK", headers, content)
    else:
        headers = Headers([("Content-Type", "text/plain")])
        return Response(404, "Not Found", headers, f"File '{file_path}' not found".encode("utf-8"))

# Обробка WebSocket (Передає ходи з браузера в модем)
async def ws_handler(websocket):
    clients.add(websocket)
    try:
        async for message in websocket:
            print(f"[ГРА -> PYTHON]: {message}")
            
            # Парсимо повідомлення формату: "НікСуперника|ПакетДаних"
            if "|" in message:
                target_nick, payload = message.split("|", 1)
                
                if target_nick == "BROADCAST" or target_nick == "":
                    print(f"📡 Відправка Broadcast: {payload}")
                    interface.sendText(payload)
                else:
                    # Шукаємо Hex ID ноди за її коротким ніком у базі модема
                    dest_id = None
                    if interface.nodes:
                        for node in interface.nodes.values():
                            user = node.get('user', {})
                            if user.get('shortName') == target_nick:
                                dest_id = user.get('id')
                                break
                    
                    if dest_id:
                        print(f"📡 Відправка Direct до {target_nick} ({dest_id}): {payload}")
                        interface.sendText(payload, destinationId=dest_id)
                    else:
                        print(f"❌ Користувача {target_nick} не знайдено в базі модема!")
                        await websocket.send(f"SYS:ERROR:Користувача {target_nick} не знайдено в базі модема.")
    finally:
        clients.remove(websocket)

# Роутер (Розділяє WebSocket рукостискання та HTTP запити)
async def router(connection, request):
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None  # Передаємо з'єднання у ws_handler
    return await http_handler(request)

# Колбек від Meshtastic (Працює в окремому потоці бібліотеки при отриманні сигналу з ефіру)
def on_receive(packet, interface):
    decoded = packet.get('decoded', {})
    if 'text' in decoded:
        text = decoded['text']
        
        # Пропускаємо пакети для всіх наших ігор
        if text.startswith(("MB:", "CHESS:", "CHECKERS:", "CHAT:")):
            sender_id = packet.get('fromId')
            sender_nick = "Unknown"
            
            if interface.nodes and sender_id in interface.nodes:
                sender_nick = interface.nodes[sender_id].get('user', {}).get('shortName', sender_id)
            
            print(f"[РАДІО -> ГРА]: {text} від {sender_nick}")
            
            # Безпечно прокидаємо повідомлення в наш головний асинхронний потік
            if loop:
                for ws in list(clients):
                    asyncio.run_coroutine_threadsafe(ws.send(text), loop)

async def main():
    global interface, loop
    loop = asyncio.get_running_loop()
    
    print("🔌 Шукаю Meshtastic на USB...")
    try:
        interface = meshtastic.serial_interface.SerialInterface()
        pub.subscribe(on_receive, "meshtastic.receive")
        print("✅ Модем підключено успішно!")
    except Exception as e:
        print(f"❌ Помилка підключення модема: {e}")
        return

    print("🚀 Сервер запущено. Відкрийте в браузері: http://127.0.0.1:8890")
    
    # Слухаємо на всіх інтерфейсах (0.0.0.0) та порті 8890
    async with websockets.serve(ws_handler, "0.0.0.0", 8890, process_request=router):
        await asyncio.Future() 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Зупинка сервера...")
    finally:
        if interface:
            interface.close()
