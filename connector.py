from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread, Lock
from contextlib import contextmanager
import socket
import struct
import time

SOCKET_TIMEOUT = 10
HEADER_LEN = 4


@contextmanager
def serve(endpoint):
  server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  server.bind(endpoint)
  server.settimeout(10)
  server.listen()
  yield server
  server.close()

def read_sock(socket):
  header = socket.recv(HEADER_LEN).replace(b'\0', b'').decode()
  if not header:
    return
  header = struct.unpack('!L', header)[1]
  return socket.recv(header)

def write_sock(socket, data):
  if socket.send(struct.pack('!L', len(data)) > 0:
    return socket.send(data)

def listen_server(sender, terminate, sock, receivers):
  executor = ThreadPoolExecutor(max_workers=64)
  with sender['lock']:
    sock.settimeout(10)
    # --
    while not terminate.is_set():
      if sender['disconnect'].is_set():
        time.sleep(1)
        continue
      break
    while ( not terminate.is_set() ) and ( not sender['disconnect'].is_set() ):
      try:
        data = read_sock(sock) # read server data
        if not data:
          print('Empty response from sender! closing.')
          sock.close()
          break # check dropped sender connection
        with receivers['lock']:
          dropped = list()
          threads = executor.map(lambda sockk: write_sock(sockk, data), receivers['connected'])
          for res in threads:
            ok, rsock = res
            if not ok:
              dropped.append(rsock)
          for d in dropped:
            d.close()
            receivers['connected'].remove(d)
            print(f'connection<{d}> dropped.')
          # -- send server display message
          if not write_sock(sock, f'Connected receivers: {len(receivers["connected"])}'.encode('utf8')):
            sock.close()
            break
      except TimeoutError:
        continue
      except KeyboardInterrupt:
        sock.close()
        terminate.set()
      except Exception as e:
        print(e)
        sock.close()
        break
  sender['disconnect'].clear()
  executor.shutdown()
  return

def start():
  receivers = {
    'lock': Lock(),
    'connected': list()
  }
  sender = {
    'lock': Lock(),
    'disconnect': Event()
  }
  terminate = Event()
  endpoint = ('127.0.0.1', 6070)
  print(f'Connector listening at: {endpoint[0]}:{endpoint[1]}')
  with serve(endpoint=endpoint) as sock:
    while not terminate.is_set():
      try:
        conn, addr = sock.accept()
        header = conn.recv(128).decode('utf8').split('|')
        theid, role = header
        # --
        print(f'{role}({theid}) connected from {addr}')
        if role == 'Sender':
          if sender['lock'].locked():
            sender['disconnect'].set()
          # -- start a new sender session
          t = Thread(target=listen_server, args=(sender, terminate, conn, receivers))
          t.name = role
          t.start()
        else:
          with receivers['lock']:
            receivers['connected'].append(conn)
      except TimeoutError:
        pass


if __name__ == '__main__':
  print('[ ctrl+c to stop ]')
  start()
