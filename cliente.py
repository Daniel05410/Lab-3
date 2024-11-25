import socket
import pickle
import random
import re
import threading
import time

def load_vector_from_file(file_path):
    try:
        with open(file_path, "r") as file:
            content = file.read()
        numbers = [float(num) if "." in num else int(num) for num in re.findall(r"-?\d+(?:\.\d+)?", content)]
        if not numbers:
            print(f"[CLIENTE] Error: No se encontraron números válidos en '{file_path}'.")
            return None
        return numbers
    except FileNotFoundError:
        print(f"[CLIENTE] Error: Archivo '{file_path}' no encontrado.")
        return None
    except Exception as e:
        print(f"[CLIENTE] Error inesperado: {e}")
        return None

def listen_for_response(port_to_listen, timeout, client_ip):
    #Función que escucha el puerto donde el cliente recibirá la respuesta.
    print("[CLIENTE] Esperando respuesta del worker...")
    try:
        # Configurar el socket para escuchar respuestas
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.settimeout(timeout)  # Tiempo máximo de espera para recibir la respuesta
            server_socket.bind((client_ip, port_to_listen))
            server_socket.listen(1)
            print(f"[CLIENTE] Esperando conexiones en el puerto {port_to_listen}...")
            conn, addr = server_socket.accept()
            with conn:
                print(f"[CLIENTE] Conexión recibida de {addr}")
                data = receive_full_data(conn)
                if data:
                    response = pickle.loads(data)
                    if "sorted_vector" in response:
                        # Mostrar el vector recibido
                        print(f"[CLIENTE] Vector ordenado recibido:")
                        print(response["sorted_vector"][:100], "...")  # Mostrar los 100 primeros elementos
    except socket.timeout:
        print("[CLIENTE] Tiempo de espera agotado. No se recibió ninguna respuesta del worker.")
    except Exception as e:
        print(f"[CLIENTE] Error al recibir respuesta: {e}")

def client(workers):
    print("Seleccione algoritmo de ordenamiento:")
    print("1. Mergesort")
    print("2. Heapsort")
    print("3. Quicksort")
    choice = int(input("Ingrese opción: "))
    algorithm = {1: "mergesort", 2: "heapsort", 3: "quicksort"}[choice]

    print("\nElija cómo se dará el vector:")
    print("1. Generar un vector aleatorio")
    print("2. Cargar archivo .txt")
    vector_choice = int(input("Ingrese elección: "))

    if vector_choice == 1:
        n = int(input("Ingrese el tamaño del vector (e.g., 10000): "))
        vector = [random.randint(1, 1000) for _ in range(n)]
        print(f"[CLIENTE] Vector de tamaño {n} generado")
    elif vector_choice == 2:
        file_path = input("Ingrese la ruta del archivo .txt: ")
        vector = load_vector_from_file(file_path)
        if vector is None:
            print("[CLIENTE] Saliendo debido a error de archivo.")
            return
        print(f"[CLIENTE] Vector de tamaño {len(vector)} cargado desde archivo")
    else:
        print("[CLIENTE] Opción inválida. Saliendo del programa.")
        return

    time_limit = float(input("Ingrese el tiempo límite por worker (en segundos): "))
    port_to_listen = int(input("Ingrese el puerto donde espera recibir la respuesta: "))
    client_ip = input("Ingrese la IP del cliente: ")
    timeout = 1000  # Tiempo de espera para recibir la respuesta

    task = {
        "algorithm": algorithm,
        "vector": vector,
        "time_limit": time_limit
    }

    # Medir el tiempo total de ida y vuelta
    total_start_time = time.time()

    # Iniciar un hilo para escuchar las respuestas del worker
    listener_thread = threading.Thread(target=listen_for_response, args=(port_to_listen, timeout, client_ip))
    listener_thread.start()

    # Conectar con el worker para enviar la tarea
    current_worker = 0
    try:
        worker_host, worker_port = workers[current_worker]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"[CLIENTE] Conectando a worker {current_worker} ({worker_host}:{worker_port})...")
            s.connect((worker_host, worker_port))
            send_full_data(s, task)
            print(f"[CLIENTE] Tarea enviada al worker {current_worker}.")
    except Exception as e:
        print(f"[CLIENTE] Error al comunicarse con el worker {current_worker}: {e}")

    listener_thread.join()  # Esperar a que el hilo de escucha termine

    # Calcular el tiempo total transcurrido
    total_elapsed_time = time.time() - total_start_time
    print(f"[CLIENTE] Tiempo total de ida y vuelta: {total_elapsed_time:.2f} segundos")

def send_full_data(sock, data):
    #Envía los datos a través del socket.
    serialized_data = pickle.dumps(data)
    sock.sendall(len(serialized_data).to_bytes(4, 'big'))  # Enviar el tamaño del mensaje primero
    sock.sendall(serialized_data)  # Luego enviar los datos serializados

def receive_full_data(sock):
    #Recibe los datos completos desde el socket.
    raw_msglen = sock.recv(4)
    if not raw_msglen:
        return None
    msglen = int.from_bytes(raw_msglen, 'big')
    buffer = b""
    while len(buffer) < msglen:
        part = sock.recv(4096)
        if not part:
            break
        buffer += part
    return buffer

if __name__ == "__main__":
    n_workers = int(input("Ingrese el número de workers disponibles: "))
    workers = []
    for i in range(n_workers):
        host = input(f"Ingrese la IP del worker {i}: ")
        port = int(input(f"Ingrese el puerto del worker {i}: "))
        workers.append((host, port))
    client(workers)