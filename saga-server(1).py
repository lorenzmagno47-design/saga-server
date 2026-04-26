import asyncio
import json
import random
import string
import websockets
from datetime import datetime

# Store rooms and connections
rooms = {}  # room_code -> room_data
connections = {}  # websocket -> {room_code, player_id, player_name}

def generate_room_code():
    while True:
        code = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ', k=4))
        if code not in rooms:
            return code

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

async def send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except:
        pass

async def broadcast_room(room_code, data, exclude=None):
    if room_code not in rooms:
        return
    room = rooms[room_code]
    for ws in list(room['connections']):
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
            # Create a new room
            room_code = generate_room_code()
            player_id = payload.get('playerId')
            player_name = payload.get('playerName')
            max_players = payload.get('maxPlayers', 4)
            color = payload.get('color', '#c9a84c')

            rooms[room_code] = {
                'code': room_code,
                'host_id': player_id,
                'max_players': max_players,
                'players': [{
                    'id': player_id,
                    'name': player_name,
                    'isHost': True,
                    'color': color,
                }],
                'connections': [ws],
                'state': 'waiting',
                'submitted_scenarios': [],
                'votes': {},
                'story_messages': [],
                'story_history': [],
                'current_player_index': 0,
                'turn_count': 0,
                'round_count': 1,
                'scenario': '',
                'genre': None,
            }

            connections[ws] = {
                'room_code': room_code,
                'player_id': player_id,
                'player_name': player_name,
            }

            await send(ws, {
                'type': 'room_created',
                'data': {
                    'roomCode': room_code,
                    'players': rooms[room_code]['players'],
                    'maxPlayers': max_players,
                }
            })
            log(f"Room {room_code} created by {player_name}")

        elif msg_type == 'join_room':
            room_code = payload.get('roomCode', '').upper()
            player_id = payload.get('playerId')
            player_name = payload.get('playerName')
            color = payload.get('color', '#4bc86a')

            if room_code not in rooms:
                await send(ws, {'type': 'error', 'data': {'message': 'Room not found!'}})
                return

            room = rooms[room_code]

            if len(room['players']) >= room['max_players']:
                await send(ws, {'type': 'error', 'data': {'message': 'Room is full!'}})
                return

            if room['state'] != 'waiting':
                await send(ws, {'type': 'error', 'data': {'message': 'Game already started!'}})
                return

            new_player = {
                'id': player_id,
                'name': player_name,
                'isHost': False,
                'color': color,
            }

            room['players'].append(new_player)
            room['connections'].append(ws)

            connections[ws] = {
                'room_code': room_code,
                'player_id': player_id,
                'player_name': player_name,
            }

            # Send full room state to new player
            await send(ws, {
                'type': 'room_joined',
                'data': {
                    'roomCode': room_code,
                    'players': room['players'],
                    'maxPlayers': room['max_players'],
                }
            })

            # Notify others
            await broadcast_room(room_code, {
                'type': 'player_joined',
                'data': new_player,
            }, exclude=ws)

            log(f"{player_name} joined room {room_code}")

        elif msg_type == 'start_scenario':
            conn = connections.get(ws)
            if not conn: return
            room_code = conn['room_code']
            room = rooms.get(room_code)
            if not room: return
            room['state'] = 'scenario'
            await broadcast_all(room_code, {'type': 'start_scenario', 'data': {}})

        elif msg_type == 'submit_scenario':
            conn = connections.get(ws)
            if not conn: return
            room_code = conn['room_code']
            room = rooms.get(room_code)
            if not room: return

            scenario = payload
            # Avoid duplicates
            if not any(s['playerId'] == scenario['playerId'] for s in room['submitted_scenarios']):
                room['submitted_scenarios'].append(scenario)

            await broadcast_all(room_code, {
                'type': 'scenario_submitted',
                'data': scenario,
            })

            # If all players submitted, start voting
            if len(room['submitted_scenarios']) >= len(room['players']):
                room['state'] = 'voting'
                await broadcast_all(room_code, {
                    'type': 'start_voting',
                    'data': {'scenarios': room['submitted_scenarios']},
                })
            log(f"Scenario submitted in room {room_code}")

        elif msg_type == 'cast_vote':
            conn = connections.get(ws)
            if not conn: return
            room_code = conn['room_code']
            room = rooms.get(room_code)
            if not room: return

            player_id = conn['player_id']
            scenario_player_id = payload.get('scenarioPlayerId')

            # Only allow one vote per player
            if player_id not in room['votes']:
                room['votes'][player_id] = scenario_player_id
                for s in room['submitted_scenarios']:
                    if s['playerId'] == scenario_player_id:
                        s['votes'] = s.get('votes', 0) + 1

                await broadcast_all(room_code, {
                    'type': 'vote_cast',
                    'data': {
                        'scenarioPlayerId': scenario_player_id,
                        'scenarios': room['submitted_scenarios'],
                    },
                })

        elif msg_type == 'start_game':
            conn = connections.get(ws)
            if not conn: return
            room_code = conn['room_code']
            room = rooms.get(room_code)
            if not room: return

            room['state'] = 'playing'
            room['scenario'] = payload.get('scenario', '')
            room['genre'] = payload.get('genre', {})
            room['current_player_index'] = 0
            room['turn_count'] = 0
            room['round_count'] = 1
            room['story_messages'] = []
            room['story_history'] = []

            await broadcast_all(room_code, {
                'type': 'start_game',
                'data': {
                    'scenario': room['scenario'],
                    'genre': room['genre'],
                    'players': room['players'],
                },
            })
            log(f"Game started in room {room_code}")

        elif msg_type == 'player_action':
            conn = connections.get(ws)
            if not conn: return
            room_code = conn['room_code']
            room = rooms.get(room_code)
            if not room: return

            action_text = payload.get('text', '')
            player_name = payload.get('playerName', '')
            player_color = payload.get('playerColor', '#888')

            room['story_messages'].append({
                'role': 'user',
                'content': player_name + ': ' + action_text,
            })
            room['story_history'].append({
                'type': 'action',
                'playerName': player_name,
                'text': action_text,
            })

            # Advance turn
            room['turn_count'] += 1
            next_index = (room['current_player_index'] + 1) % len(room['players'])
            if next_index == 0:
                room['round_count'] += 1
            room['current_player_index'] = next_index

            await broadcast_all(room_code, {
                'type': 'player_action',
                'data': {
                    'text': action_text,
                    'playerName': player_name,
                    'playerColor': player_color,
                    'currentPlayerIndex': room['current_player_index'],
                    'turnCount': room['turn_count'],
                    'roundCount': room['round_count'],
                },
            })

        elif msg_type == 'narration':
            conn = connections.get(ws)
            if not conn: return
            room_code = conn['room_code']
            room = rooms.get(room_code)
            if not room: return

            narration_text = payload.get('text', '')
            room['story_messages'].append({
                'role': 'assistant',
                'content': narration_text,
            })
            room['story_history'].append({
                'type': 'narration',
                'text': narration_text,
            })

            await broadcast_room(room_code, {
                'type': 'narration',
                'data': {
                    'text': narration_text,
                    'currentPlayerIndex': room['current_player_index'],
                    'turnCount': room['turn_count'],
                    'roundCount': room['round_count'],
                },
            }, exclude=ws)  # exclude sender since they already have it

        elif msg_type == 'end_game':
            conn = connections.get(ws)
            if not conn: return
            room_code = conn['room_code']
            room = rooms.get(room_code)
            if not room: return
            room['state'] = 'ended'
            await broadcast_room(room_code, {
                'type': 'game_ended',
                'data': {},
            }, exclude=ws)

        elif msg_type == 'ping':
            await send(ws, {'type': 'pong', 'data': {}})

    except json.JSONDecodeError:
        await send(ws, {'type': 'error', 'data': {'message': 'Invalid JSON'}})
    except Exception as e:
        log(f"Error handling message: {e}")

async def handle_disconnect(ws):
    conn = connections.pop(ws, None)
    if not conn:
        return

    room_code = conn['room_code']
    player_id = conn['player_id']
    player_name = conn['player_name']

    room = rooms.get(room_code)
    if not room:
        return

    # Remove from connections list
    if ws in room['connections']:
        room['connections'].remove(ws)

    # Remove from players list
    room['players'] = [p for p in room['players'] if p['id'] != player_id]

    log(f"{player_name} disconnected from room {room_code}")

    # Notify others
    await broadcast_room(room_code, {
        'type': 'player_left',
        'data': {'playerId': player_id, 'playerName': player_name, 'players': room['players']},
    })

    # Clean up empty rooms
    if not room['players']:
        rooms.pop(room_code, None)
        log(f"Room {room_code} closed (empty)")
    elif player_id == room['host_id'] and room['players']:
        # Transfer host
        new_host = room['players'][0]
        new_host['isHost'] = True
        room['host_id'] = new_host['id']
        await broadcast_room(room_code, {
            'type': 'host_changed',
            'data': {'newHostId': new_host['id'], 'players': room['players']},
        })
        log(f"Host transferred to {new_host['name']} in room {room_code}")

async def handler(ws):
    log(f"New connection: {ws.remote_address}")
    try:
        async for message in ws:
            await handle_message(ws, message)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await handle_disconnect(ws)
        log(f"Connection closed: {ws.remote_address}")

async def main():
    print("=" * 40)
    print("  SAGA RPG WebSocket Server")
    print("  Running on port {port}")
    print("=" * 40)
    import os
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
