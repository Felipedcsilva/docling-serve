import runpod
import requests
import subprocess
import threading
import time
import os
import base64
import json
import traceback # Adicionado para stack traces

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
    print(" Tentando iniciar o servidor docling-serve...")
    try:
        docling_server_process = subprocess.Popen(
            ["docling-serve", "run"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line-buffered
            universal_newlines=True
        )
        print(f" Subprocesso docling-serve iniciado com PID: {docling_server_process.pid}")

        # Threads para consumir stdout e stderr do subprocesso e logá-los
        def log_stream(stream, stream_name):
            try:
                for line in iter(stream.readline, ''):
                    print(f"docling-serve [{stream_name}]: {line.strip()}")
                stream.close()
            except Exception as e:
                print(f"Erro ao logar stream {stream_name} do docling-serve: {e}\n{traceback.format_exc()}")

        stdout_thread = threading.Thread(target=log_stream, args=(docling_server_process.stdout, 'stdout'), daemon=True)
        stderr_thread = threading.Thread(target=log_stream, args=(docling_server_process.stderr, 'stderr'), daemon=True)
        stdout_thread.start()
        stderr_thread.start()

        # Verificação mais robusta se o servidor está pronto
        # Tenta conectar ao endpoint de health/docs por um tempo
        # Isso precisa que o docling-serve tenha um endpoint que responda rapidamente quando estiver ok
        # Ex: /docs ou um /healthz. Se não, o sleep é a alternativa mais simples.
        print(" Aguardando docling-serve ficar pronto...")
        time.sleep(20) # Aumentado o tempo, e os logs acima ajudarão a ver se ele inicia
        # Aqui você poderia adicionar uma lógica para verificar se o Popen ainda está rodando:
        if docling_server_process.poll() is not None:
            print(f" ERRO: Subprocesso docling-serve terminou prematuramente com código {docling_server_process.returncode}")
            # Os logs de stderr já devem ter mostrado o erro
            return # Não define o evento started
        
        print(" Servidor docling-serve presumivelmente iniciado e pronto.")
        docling_server_started.set()

    except FileNotFoundError:
        print(f" ERRO CRÍTICO: Comando 'docling-serve' não encontrado. Verifique o PATH e a instalação.\n{traceback.format_exc()}")
    except Exception as e:
        print(f" ERRO CRÍTICO ao tentar iniciar docling-serve: {e}\n{traceback.format_exc()}")
        # Se o Popen falhar, o stderr não será capturado pela thread acima, então logamos aqui.
        if hasattr(e, 'stderr') and e.stderr:
            print(f"Erro (stderr do subprocesso): {e.stderr.decode() if isinstance(e.stderr, bytes) else e.stderr}")


def ensure_docling_server_is_running():
    global docling_server_process
    print(" Verificando se o servidor docling está rodando...")
    with docling_server_lock:
        if not docling_server_process and not docling_server_started.is_set():
            print(" Servidor docling não iniciado, chamando start_docling_server().")
            server_thread = threading.Thread(target=start_docling_server, daemon=True)
            server_thread.start()
        elif docling_server_process and docling_server_process.poll() is not None:
            print(f" ERRO: Tentativa de usar docling-serve, mas o subprocesso já terminou com código {docling_server_process.returncode}. Tentando reiniciar...")
            docling_server_started.clear()
            # A variável já foi declarada global no escopo desta função
            docling_server_process = None 
            server_thread = threading.Thread(target=start_docling_server, daemon=True)
            server_thread.start()
            
    print(" Aguardando sinal de 'docling_server_started'...")
    docling_server_started.wait(timeout=70) # Timeout aumentado
    if not docling_server_started.is_set():
        print(" ERRO: Timeout esperando o sinal 'docling_server_started'. O servidor pode não ter iniciado.")
        raise RuntimeError("Servidor Docling não iniciou ou não sinalizou pronto a tempo.")
    print(" Sinal 'docling_server_started' recebido. Servidor Docling está (ou deveria estar) pronto.")

# --- Handler do RunPod ---
def process_document_conversion(job):
    try:
        print(f" Recebido job: {job.get('id', 'N/A')}")
        ensure_docling_server_is_running()
        job_input = job.get('input', {})
        if not job_input:
            print(" Job input vazio.")
            return {"error": "Nenhum input fornecido."}

        file_content_base64 = job_input.get('file_content_base64')
        filename = job_input.get('filename', 'uploaded_file')
        options = job_input.get('options', {})

        if not file_content_base64:
            print(" file_content_base64 não encontrado no input.")
            return {"error": "Conteúdo do arquivo (file_content_base64) não fornecido."}

        try:
            file_content_bytes = base64.b64decode(file_content_base64)
        except Exception as e:
            print(f"Erro ao decodificar Base64: {e}")
            return {"error": f"Falha ao decodificar o conteúdo do arquivo Base64: {e}"}

        if not file_content_bytes:
            print(" Conteúdo do arquivo decodificado vazio.")
            return {"error": "Conteúdo do arquivo decodificado está vazio."}

        print(f"Enviando arquivo '{filename}' para conversão com opções: {options}")
        
        # Preparar o arquivo para upload - usando "files" conforme exigido pela API
        files_payload = {'files': (filename, file_content_bytes)}
        
        # Preparar as opções
        data_payload = {}
        
        # Tentando enviar opções diretamente como parâmetros de formulário
        if 'to_formats' in options:
            data_payload['to_formats'] = ','.join(options['to_formats'])
        if 'ocr' in options:
            data_payload['ocr'] = str(options['ocr']).lower()  # "true" ou "false" em string

        # Removendo options_str que pode causar conflito
        # if options:
        #     try:
        #         data_payload['options_str'] = json.dumps(options)
        #         
        #         print(f"Payload de dados preparado: {data_payload}")
        #     except TypeError as e:
        #         print(f"Erro ao serializar opções JSON: {e}")
        #         return {"error": f"Erro ao serializar opções para JSON: {e}", "options_fornecidas": options}

        print(f"Payload de dados preparado: {data_payload}")

        try:
            headers = {
                'Accept': 'application/json',
            }
            
            print(f"Enviando para {CONVERT_ENDPOINT} com data: {data_payload}")
            response = requests.post(CONVERT_ENDPOINT, files=files_payload, data=data_payload, headers=headers, timeout=30)
            print(f" Chamada para CONVERT_ENDPOINT. Status: {response.status_code}, Resposta: {response.text[:200]}...")
            
            if response.status_code == 422:
                print(f" ERRO 422: Resposta completa: {response.text}")
                return {"error": f"Erro 422 Unprocessable Entity: O servidor não conseguiu processar a requisição. Detalhes: {response.text}"}
            
            response.raise_for_status()
            convert_data = response.json()
            task_id = convert_data.get('task_id')
            if not task_id:
                print(" task_id não recebido.")
                return {"error": "task_id não encontrado na resposta de conversão.", "details": convert_data}
            print(f"Conversão iniciada. Task ID: {task_id}")
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição de conversão: {e}")
            return {"error": f"Erro ao chamar o endpoint de conversão: {e}"}
        except ValueError as e: # JSONDecodeError
            print(f"Erro ao decodificar JSON da resposta de conversão: {e}")
            return {"error": "Resposta de conversão não é um JSON válido.", "content": response.text}

        status_url = f"{STATUS_ENDPOINT_TPL}{task_id}"
        max_polls = 60
        poll_interval = 5
        print(f"Iniciando polling para Task ID: {task_id}")
        for i in range(max_polls):
            try:
                status_response = requests.get(status_url, timeout=10)
                print(f"Poll {i+1}/{max_polls} para {task_id}. Status: {status_response.status_code}, Resposta: {status_response.text[:200]}...")
                status_response.raise_for_status()
                status_data = status_response.json()
                current_status = status_data.get('task_status', '').upper()
                print(f"Poll {i+1}/{max_polls}: Status atual = {current_status}, Detalhes: {status_data}")
                if current_status == "SUCCESS" or current_status == "COMPLETED":
                    break
                elif current_status == "FAILURE" or current_status == "FAILED":
                    print(f" Conversão falhou para {task_id}.")
                    return {"error": f"Conversão falhou para Task ID {task_id}.", "status_details": status_data}
                time.sleep(poll_interval)
            except requests.exceptions.RequestException as e:
                print(f"Erro durante o polling de status para {task_id}: {e}")
                time.sleep(poll_interval)
            except ValueError as e: # JSONDecodeError
                print(f"Erro ao decodificar JSON da resposta de status para {task_id}: {e}")
                return {"error": f"Resposta de status para Task ID {task_id} não é um JSON válido.", "content": status_response.text}
        else:
            print(f" Timeout no polling para {task_id}.")
            return {"error": f"Timeout durante o polling de status para Task ID {task_id}."}

        print(f"Conversão concluída para Task ID: {task_id}. Buscando resultado...")
        result_url = f"{RESULT_ENDPOINT_TPL}{task_id}"
        try:
            result_response = requests.get(result_url, timeout=30)
            print(f" Chamada para RESULT_ENDPOINT para {task_id}. Status: {result_response.status_code}, Resposta (preview): {result_response.text[:200]}...")
            result_response.raise_for_status()
            final_result = result_response.json()
            print(f"Resultado obtido para Task ID: {task_id}")
            return final_result
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar resultado para {task_id}: {e}")
            return {"error": f"Erro ao buscar o resultado para Task ID {task_id}: {e}"}
        except ValueError as e: # JSONDecodeError
            print(f"Erro ao decodificar JSON do resultado para {task_id}: {e}")
            return {"error": f"Resultado para Task ID {task_id} não é um JSON válido.", "content_type": result_response.headers.get('Content-Type'), "raw_content_preview": result_response.text[:200]}

    except Exception as e:
        print(f" ERRO INESPERADO no handler process_document_conversion: {e}\n{traceback.format_exc()}")
        return {
            "error": f"Erro inesperado no handler: {str(e)}",
            "traceback": traceback.format_exc()
        }

if __name__ == "__main__":
    try:
        print(" Iniciando script handler.py...")
        ensure_docling_server_is_running() # Tenta iniciar o servidor uma vez na inicialização do script
        print(" Iniciando o listener do RunPod serverless...")
        runpod.serverless.start({"handler": process_document_conversion})
    except Exception as e:
        print(f" ERRO CRÍTICO na inicialização do handler.py (bloco __main__): {e}\n{traceback.format_exc()}")
        # Se falhar aqui, o worker provavelmente sairá com erro.
        # É importante que este log apareça para diagnóstico.
        # Considerar sair explicitamente com exit(1) se for um erro irrecuperável. 