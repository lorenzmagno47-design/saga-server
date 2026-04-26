import asyncio
import json
import random
import os
import websockets
from datetime import datetime

rooms = {}
connections = {}

def generate_room_code():
    while True:
        code = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ', k=4))
        if code not in rooms:
            return code

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

async def send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except:
        pass

async def broadcast_room(room_code, data, exclude=None):
    if room_code not in rooms:
        return
    for ws in list(rooms[room_code]['connections']):
        if ws != exclude:
            await send(ws, data)

async def broadcast_all(room_code, data):
    await broadcast_room(room_code, data)

async def handle_message(ws, message):
    try:
        data = json.loads(message)
        msg_type = data.get('type')
        payload = data.get('data', {})

        if msg_type == 'create_room':
            room_code = generate_room_code()
            player_id = payload.get('playerId')
            player_name = payload.get('playerName')
            max_players = payload.get('maxPlayers', 4)
            color = payload.get('color', '#c9a84c')
            rooms[room_code] = {
                'code': room_code, 'host_id': player_id, 'max_players': max_players,
                'players': [{'id': player_id, 'name': player_name, 'isHost': True, 'color': color}],
                'connections': [ws], 'state': 'waiting', 'submitted_scenarios': [],
                'votes': {}, 'story_messages': [], 'story_history': [],
                'current_player_index': 0, 'turn_count': 0, 'round_count': 1,
                'scenario': '', 'genre': None,
            }
            connections[ws] = {'room_code': room_code, 'player_id': player_id, 'player_name': player_name}
            await send(ws, {'type': 'room_created', 'data': {
                'roomCode': room_code, 'players': rooms[room_code]['players'], 'maxPlayers': max_players,
            }})
            log(f"Room {room_code} created by {player_name}")

        elif msg_type == 'join_room':
            room_code = payload.get('roomCode', '').upper()
            player_id = payload.get('playerId')
            player_name = payload.get('playerName')
            color = payload.get('color', '#4bc86a')
            if room_code not in rooms:
                await send(ws, {'type': 'error', 'data': {'message': 'Room not found!'}}); return
            room = rooms[room_code]
            if len(room['players']) >= room['max_players']:
                await send(ws, {'type': 'error', 'data': {'message': 'Room is full!'}}); return
            if room['state'] != 'waiting':
                await send(ws, {'type': 'error', 'data': {'message': 'Game already started!'}}); return
            new_player = {'id': player_id, 'name': player_name, 'isHost': False, 'color': color}
            room['players'].append(new_player)
            room['connections'].append(ws)
            connections[ws] = {'room_code': room_code, 'player_id': player_id, 'player_name': player_name}
            await send(ws, {'type': 'room_joined', 'data': {
                'roomCode': room_code, 'players': room['players'], 'maxPlayers': room['max_players'],
            }})
            await broadcast_room(room_code, {'type': 'player_joined', 'data': new_player}, exclude=ws)
            log(f"{player_name} joined room {room_code}")

        elif msg_type == 'start_scenario':
            conn = connections.get(ws)
            if not conn: return
            rooms[conn['room_code']]['state'] = 'scenario'
            await broadcast_all(conn['room_code'], {'type': 'start_scenario', 'data': {}})

        elif msg_type == 'submit_scenario':
            conn = connections.get(ws)
            if not conn: return
            room = rooms.get(conn['room_code'])
            if not room: return
            if not any(s['playerId'] == payload['playerId'] for s in room['submitted_scenarios']):
                room['submitted_scenarios'].append(payload)
            await broadcast_all(conn['room_code'], {'type': 'scenario_submitted', 'data': payload})
            if len(room['submitted_scenarios']) >= len(room['players']):
                room['state'] = 'voting'
                await broadcast_all(conn['room_code'], {'type': 'start_voting', 'data': {'scenarios': room['submitted_scenarios']}})

        elif msg_type == 'cast_vote':
            conn = connections.get(ws)
            if not conn: return
            room = rooms.get(conn['room_code'])
            if not room: return
            player_id = conn['player_id']
            scenario_player_id = payload.get('scenarioPlayerId')
            if player_id not in room['votes']:
                room['votes'][player_id] = scenario_player_id
                for s in room['submitted_scenarios']:
                    if s['playerId'] == scenario_player_id:
                        s['votes'] = s.get('votes', 0) + 1
                await broadcast_all(conn['room_code'], {'type': 'vote_cast', 'data': {
                    'scenarioPlayerId': scenario_player_id, 'scenarios': room['submitted_scenarios'],
                }})

        elif msg_type == 'start_game':
            conn = connections.get(ws)
            if not conn: return
            room = rooms.get(conn['room_code'])
            if not room: return
            room['state'] = 'playing'
            room['scenario'] = payload.get('scenario', '')
            room['genre'] = payload.get('genre', {})
            room['current_player_index'] = 0
            room['turn_count'] = 0
            room['round_count'] = 1
            room['story_messages'] = []
            room['story_history'] = []
            await broadcast_all(conn['room_code'], {'type': 'start_game', 'data': {
                'scenario': room['scenario'], 'genre': room['genre'], 'players': room['players'],
            }})
            log(f"Game started in room {conn['room_code']}")

        elif msg_type == 'player_action':
            conn = connections.get(ws)
            if not conn: return
            room = rooms.get(conn['room_code'])
            if not room: return
            room['turn_count'] += 1
            next_index = (room['current_player_index'] + 1) % len(room['players'])
            if next_index == 0: room['round_count'] += 1
            room['current_player_index'] = next_index
            room['story_messages'].append({'role': 'user', 'content': payload.get('playerName', '') + ': ' + payload.get('text', '')})
            await broadcast_all(conn['room_code'], {'type': 'player_action', 'data': {
                'text': payload.get('text'), 'playerName': payload.get('playerName'),
                'playerColor': payload.get('playerColor'),
                'currentPlayerIndex': room['current_player_index'],
                'turnCount': room['turn_count'], 'roundCount': room['round_count'],
            }})

        elif msg_type == 'narration':
            conn = connections.get(ws)
            if not conn: return
            room = rooms.get(conn['room_code'])
            if not room: return
            room['story_messages'].append({'role': 'assistant', 'content': payload.get('text', '')})
            await broadcast_room(conn['room_code'], {'type': 'narration', 'data': {
                'text': payload.get('text'),
                'currentPlayerIndex': room['current_player_index'],
                'turnCount': room['turn_count'], 'roundCount': room['round_count'],
            }}, exclude=ws)

        elif msg_type == 'end_game':
            conn = connections.get(ws)
            if not conn: return
            if conn['room_code'] in rooms: rooms[conn['room_code']]['state'] = 'ended'
            await broadcast_room(conn['room_code'], {'type': 'game_ended', 'data': {}}, exclude=ws)

        elif msg_type == 'ping':
            await send(ws, {'type': 'pong', 'data': {}})

    except Exception as e:
        log(f"Error handling message: {e}")

async def handle_disconnect(ws):
    conn = connections.pop(ws, None)
    if not conn: return
    room_code = conn['room_code']
    player_id = conn['player_id']
    player_name = conn['player_name']
    room = rooms.get(room_code)
    if not room: return
    if ws in room['connections']: room['connections'].remove(ws)
    room['players'] = [p for p in room['players'] if p['id'] != player_id]
    log(f"{player_name} disconnected from {room_code}")
    await broadcast_room(room_code, {'type': 'player_left', 'data': {
        'playerId': player_id, 'playerName': player_name, 'players': room['players'],
    }})
    if not room['players']:
        rooms.pop(room_code, None)
        log(f"Room {room_code} closed")
    elif player_id == room['host_id'] and room['players']:
        new_host = room['players'][0]
        new_host['isHost'] = True
        room['host_id'] = new_host['id']
        await broadcast_room(room_code, {'type': 'host_changed', 'data': {
            'newHostId': new_host['id'], 'players': room['players'],
        }})
        log(f"Host transferred to {new_host['name']} in {room_code}")

async def handler(ws):
    log(f"New connection: {ws.remote_address}")
    try:
        async for message in ws:
            await handle_message(ws, message)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await handle_disconnect(ws)

async def main():
    port = int(os.environ.get("PORT", 8765))
    log(f"SAGA RPG Server starting on port {port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        log(f"Server ready on port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
