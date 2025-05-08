import runpod
import requests
import subprocess
import threading
import time
import os
import base64
import json

# --- Configuração do Servidor Docling ---
DOCLING_HOST = "http://localhost:5001"
CONVERT_ENDPOINT = f"{DOCLING_HOST}/v1alpha/convert/file/async"
STATUS_ENDPOINT_TPL = f"{DOCLING_HOST}/v1alpha/status/poll/"
RESULT_ENDPOINT_TPL = f"{DOCLING_HOST}/v1alpha/result/"

docling_server_process = None
docling_server_started = threading.Event()
docling_server_lock = threading.Lock()

def start_docling_server():
    global docling_server_process
    try:
        print("Iniciando o servidor docling-serve...")
        # Usamos Popen para não bloquear e permitir a captura de logs se necessário
        docling_server_process = subprocess.Popen(
            ["docling-serve", "run"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Monitorar a saída para saber quando está pronto (ajuste conforme necessário)
        # Esta é uma verificação simples; pode precisar ser mais robusta
        # olhando para uma linha específica no log do docling-serve.
        time.sleep(15) # Dê um tempo generoso para o servidor iniciar
        print("Servidor docling-serve presumivelmente iniciado.")
        docling_server_started.set()
    except Exception as e:
        print(f"Falha ao iniciar docling-serve: {e}")
        # Se falhar, o worker provavelmente não será saudável.
        # Considere como lidar com isso (ex: sair do processo).

def ensure_docling_server_is_running():
    with docling_server_lock:
        if not docling_server_process and not docling_server_started.is_set():
            server_thread = threading.Thread(target=start_docling_server, daemon=True)
            server_thread.start()
    docling_server_started.wait(timeout=60) # Espere até 60s pelo sinal
    if not docling_server_started.is_set():
        raise RuntimeError("Servidor Docling não iniciou a tempo.")

# --- Handler do RunPod ---
def process_document_conversion(job):
    ensure_docling_server_is_running()

    job_input = job.get('input', {})
    if not job_input:
        return {"error": "Nenhum input fornecido."}

    file_content_base64 = job_input.get('file_content_base64')
    filename = job_input.get('filename', 'uploaded_file') # Nome de arquivo padrão
    options = job_input.get('options', {}) # Obter 'options', default para {} se não fornecido

    if not file_content_base64:
        return {"error": "Conteúdo do arquivo (file_content_base64) não fornecido."}

    try:
        file_content_bytes = base64.b64decode(file_content_base64)
    except Exception as e:
        return {"error": f"Falha ao decodificar o conteúdo do arquivo Base64: {e}"}

    if not file_content_bytes:
        return {"error": "Conteúdo do arquivo decodificado está vazio."}

    # 1. Enviar arquivo para conversão
    print(f"Enviando arquivo '{filename}' para conversão com opções: {options}")
    files_payload = {'upload_file': (filename, file_content_bytes)}
    
    data_payload = {}
    if options:
        try:
            # O docling-serve /v1alpha/convert/file/async provavelmente espera
            # as opções como uma string JSON em um campo de formulário.
            # Nomeando o campo 'options_str' por enquanto.
            data_payload['options_str'] = json.dumps(options)
        except TypeError as e:
            return {"error": f"Erro ao serializar opções para JSON: {e}", "options_fornecidas": options}

    try:
        # Passar 'options_str' no parâmetro 'data' da requisição multipart
        response = requests.post(CONVERT_ENDPOINT, files=files_payload, data=data_payload, timeout=30)
        response.raise_for_status() # Levanta exceção para códigos de erro HTTP
        convert_data = response.json()
        task_id = convert_data.get('task_id')
        if not task_id:
            return {"error": "task_id não encontrado na resposta de conversão.", "details": convert_data}
        print(f"Conversão iniciada. Task ID: {task_id}")
    except requests.exceptions.RequestException as e:
        return {"error": f"Erro ao chamar o endpoint de conversão: {e}"}
    except ValueError: # JSONDecodeError
        return {"error": "Resposta de conversão não é um JSON válido.", "content": response.text}


    # 2. Polling de status
    status_url = f"{STATUS_ENDPOINT_TPL}{task_id}"
    max_polls = 60  # Máximo de 5 minutos de polling (60 * 5s)
    poll_interval = 5  # segundos

    print(f"Iniciando polling para Task ID: {task_id}")
    for i in range(max_polls):
        try:
            status_response = requests.get(status_url, timeout=10)
            status_response.raise_for_status()
            status_data = status_response.json()
            current_status = status_data.get('status', '').upper() # Normalizar para maiúsculas
            print(f"Poll {i+1}/{max_polls}: Status atual = {current_status}, Detalhes: {status_data}")

            if current_status == "SUCCESS" or current_status == "COMPLETED": # Ajuste conforme a API do docling
                break
            elif current_status == "FAILURE" or current_status == "FAILED":
                return {"error": f"Conversão falhou para Task ID {task_id}.", "status_details": status_data}
            
            time.sleep(poll_interval)
        except requests.exceptions.RequestException as e:
            print(f"Erro durante o polling de status para Task ID {task_id}: {e}")
            # Decidir se deve continuar ou falhar. Por enquanto, continua.
            time.sleep(poll_interval) # Evita spammar em caso de erro de rede
        except ValueError:
             return {"error": f"Resposta de status para Task ID {task_id} não é um JSON válido.", "content": status_response.text}
    else: # Se o loop terminar sem break
        return {"error": f"Timeout durante o polling de status para Task ID {task_id}."}

    print(f"Conversão concluída para Task ID: {task_id}. Buscando resultado...")

    # 3. Obter resultado
    result_url = f"{RESULT_ENDPOINT_TPL}{task_id}"
    try:
        result_response = requests.get(result_url, timeout=30)
        result_response.raise_for_status()
        # O resultado pode ser JSON ou um arquivo direto.
        # Se for um arquivo, você pode querer retorná-lo como base64.
        # Se for JSON, apenas retorne.
        # Vamos assumir JSON por enquanto.
        final_result = result_response.json()
        print(f"Resultado obtido para Task ID: {task_id}")
        return final_result
    except requests.exceptions.RequestException as e:
        return {"error": f"Erro ao buscar o resultado para Task ID {task_id}: {e}"}
    except ValueError: # JSONDecodeError
        # Se não for JSON, talvez seja o arquivo direto. Você precisará decidir como lidar com isso.
        # Por exemplo, retornar o conteúdo como base64 se for um tipo de arquivo esperado.
        return {"error": f"Resultado para Task ID {task_id} não é um JSON válido.", "content_type": result_response.headers.get('Content-Type'), "raw_content_preview": result_response.text[:200]}


# Inicia o servidor docling e o handler do RunPod
if __name__ == "__main__":
    # Inicia o servidor docling em uma thread para não bloquear o runpod.serverless.start
    # A função ensure_docling_server_is_running será chamada pelo handler antes de cada job.
    # No entanto, é bom tentar iniciar uma vez aqui para prontidão.
    ensure_docling_server_is_running()
    
    print("Iniciando o handler do RunPod...")
    runpod.serverless.start({"handler": process_document_conversion}) 