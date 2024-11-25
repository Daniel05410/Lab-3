import time

def partial_mergesort_with_progress(vector, time_limit, progress=None):
    #Mergesort con progreso parcial
    start_time = time.time()

    if progress is None:
        progress = {"split": [(0, len(vector))], "merge": []}

    def merge_with_time_check(vector, left, mid, right, start_time, time_limit):
        #Fusionar dos mitades, y verificar el tiempo límite.
        sorted_subarray = []
        i, j = left, mid
        while i < mid and j < right:
            if time.time() - start_time > time_limit:
                return None, False
            if vector[i] < vector[j]:
                sorted_subarray.append(vector[i])
                i += 1
            else:
                sorted_subarray.append(vector[j])
                j += 1
        sorted_subarray.extend(vector[i:mid])
        sorted_subarray.extend(vector[j:right])
        vector[left:right] = sorted_subarray
        return vector, True

    # Cambiar el orden: realizar todas las divisiones antes de fusionar
    while progress["split"]:
        if time.time() - start_time > time_limit:
            return vector, time.time() - start_time, progress

        left, right = progress["split"].pop()
        if right - left > 1:
            mid = (left + right) // 2
            progress["split"].append((left, mid))
            progress["split"].append((mid, right))
            progress["merge"].insert(0, (left, mid, right))  # Fusiones en orden jerárquico

    # Fusiones después de completar todas las divisiones
    while progress["merge"]:
        if time.time() - start_time > time_limit:
            return vector, time.time() - start_time, progress

        left, mid, right = progress["merge"].pop(0)
        result, success = merge_with_time_check(vector, left, mid, right, start_time, time_limit)
        if not success:
            progress["merge"].insert(0, (left, mid, right))
            return vector, time.time() - start_time, progress

    elapsed_time = time.time() - start_time
    return vector, elapsed_time, None


def partial_heapsort_with_progress(vector, time_limit, progress=None):
    start_time = time.time()
    n = len(vector)

    if progress is None:
        progress = {"phase": "heapify", "i": n // 2 - 1}

    def heapify(arr, n, i):
        largest = i
        left = 2 * i + 1
        right = 2 * i + 2

        if left < n and arr[largest] < arr[left]:
            largest = left
        if right < n and arr[largest] < arr[right]:
            largest = right

        if largest != i:
            arr[i], arr[largest] = arr[largest], arr[i]
            heapify(arr, n, largest)

    if progress["phase"] == "heapify":
        for i in range(progress["i"], -1, -1):
            heapify(vector, n, i)
            progress["i"] = i
            if time.time() - start_time > time_limit:
                return vector, time.time() - start_time, progress
        progress["phase"] = "sort"
        progress["i"] = n - 1

    if progress["phase"] == "sort":
        for i in range(progress["i"], 0, -1):
            vector[i], vector[0] = vector[0], vector[i]
            heapify(vector, i, 0)
            progress["i"] = i
            if time.time() - start_time > time_limit:
                return vector, time.time() - start_time, progress

    elapsed_time = time.time() - start_time
    return vector, elapsed_time, None

def partial_quicksort_with_progress(vector, time_limit, progress=None):
    start_time = time.time()

    if progress is None:
        # Inicializar el progreso para todo el rango del vector
        progress = [(0, len(vector) - 1)]

    def partition(arr, low, high):
        #Realiza una partición del vector para quicksort
        pivot = arr[high]
        i = low - 1
        for j in range(low, high):
            if arr[j] < pivot:
                i += 1
                arr[i], arr[j] = arr[j], arr[i]
        arr[i + 1], arr[high] = arr[high], arr[i + 1]
        return i + 1

    while progress:
        if time.time() - start_time > time_limit:
            break 

        low, high = progress.pop()  # Obtener el siguiente rango de partición

        if low < high:
            pivot_index = partition(vector, low, high)  # Realizar partición
            progress.append((low, pivot_index - 1))  # Agregar la parte izquierda
            progress.append((pivot_index + 1, high))  # Agregar la parte derecha

    elapsed_time = time.time() - start_time
    return vector, elapsed_time, progress  # Retornar el vector con las particiones y el progreso