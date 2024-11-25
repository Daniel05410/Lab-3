import socket
import pickle
import time
from algoritmos import * 

def send_full_data(sock, data):
    #Enviar datos serializados a través del socket.
    try:
        serialized_data = pickle.dumps(data)
        sock.sendall(len(serialized_data).to_bytes(4, 'big'))  # Enviar el tamaño del mensaje
        sock.sendall(serialized_data)  # Enviar el mensaje serializado
    except Exception as e:
        print(f"[ERROR] Error al enviar datos: {e}")

def receive_full_data(sock):
    #Recibir datos completos a través del socket.
    try:
        raw_msglen = sock.recv(4)  # Recibir el tamaño del mensaje
        if not raw_msglen:
            return None  # Si no se recibe nada
        msglen = int.from_bytes(raw_msglen, 'big')
        buffer = b""
        while len(buffer) < msglen:
            part = sock.recv(4096)
            if not part:
                break
            buffer += part
        return pickle.loads(buffer)  # Deserializar los datos recibidos
    except Exception as e:
        print(f"[ERROR] Error al recibir datos: {e}")
        return None

def send_to_client(sock, sorted_vector, elapsed_time):
    #Enviar los datos al cliente cuando la tarea se completa.
    try:
        response = {"sorted_vector": sorted_vector, "time_taken": elapsed_time}
        send_full_data(sock, response)
        print(f"[WORKER] Enviado resultados al cliente. Tarea completada en {elapsed_time:.2f}s.")
    except Exception as e:
        print(f"[ERROR] Error al enviar resultados al cliente: {e}")

def worker(host, port, next_worker_host, next_worker_port, client_host, client_port):
    #Lógica principal del worker
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(1)
    print(f"[WORKER {port}] Esperando conexión en {host}:{port}")

    while True:
        conn, _ = server.accept()  # Aceptar una conexión entrante
        try:
            # Recibir datos del cliente o worker anterior
            data = receive_full_data(conn)
            if data is None:
                raise ConnectionError("No se pudo recibir datos del cliente o worker anterior")
            task = data 

            # Extraer detalles de la tarea
            algorithm = task["algorithm"]
            vector = task["vector"]
            time_limit = task["time_limit"]
            progress = task.get("progress", None)  # Tomar el progreso acumulado, si existe

            start_time = time.time()
            print(f"[WORKER {port}] Recibió tarea: {algorithm} con {time_limit:.2f}s restantes.")

            # Ejecución del algoritmo
            if algorithm == "mergesort":
                sorted_vector, elapsed_time, progress = partial_mergesort_with_progress(vector, time_limit, progress)
            elif algorithm == "heapsort":
                sorted_vector, elapsed_time, progress = partial_heapsort_with_progress(vector, time_limit, progress)
            elif algorithm == "quicksort":
                sorted_vector, elapsed_time, progress = partial_quicksort_with_progress(vector, time_limit, progress)
            else:
                raise ValueError(f"Algoritmo desconocido: {algorithm}")

            # Verificación del tiempo
            time_remaining = time_limit - elapsed_time
            if time_remaining > 0:
                # Si se completó el trabajo, enviar al cliente
                print(f"[WORKER {port}] Tarea completada en {elapsed_time:.2f} segundos. Enviando al cliente.")
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
                    client_sock.connect((client_host, client_port))
                    send_to_client(client_sock, sorted_vector, elapsed_time)
                break
            else:
                # Si no se completó el trabajo, pasar al siguiente worker
                print(f"[WORKER {port}] Tiempo agotado ({elapsed_time:.2f}s). Reenviando tarea al siguiente worker.")
                task["vector"] = sorted_vector
                task["time_limit"] = time_limit  # Pasar el tiempo restante
                task["progress"] = progress  # Mantener el progreso acumulado y actualizar la tarea

                # Enviar la tarea al siguiente worker con el progreso actualizado
                try:
                    with socket.create_connection((next_worker_host, next_worker_port)) as next_sock:
                        send_full_data(next_sock, task)
                except Exception as e:
                    print(f"[WORKER {port}] Error al conectar con el siguiente worker: {e}")

        except Exception as e:
            print(f"[WORKER {port}] Error durante la ejecución: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    # Solicitar IP y puerto para este worker
    host = input("Ingrese la IP de este worker: ")
    port = int(input("Ingrese el puerto de este worker: "))

    # Solicitar IP y puerto del siguiente worker
    next_worker_host = input("Ingrese la IP del siguiente worker: ")
    next_worker_port = int(input("Ingrese el puerto del siguiente worker: "))

    # Solicitar IP y puerto del cliente al que se enviará la respuesta
    client_host = input("Ingrese la IP del cliente: ")
    client_port = int(input("Ingrese el puerto del cliente: "))

    # Iniciar el worker
    worker(host, port, next_worker_host, next_worker_port, client_host, client_port)