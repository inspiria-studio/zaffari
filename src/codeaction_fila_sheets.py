import requests
import json
import time

#  692034097a06b1c824249d9d  - fila

def push_fila(numero_pedido, json_data, web_app_url, engine):
    """
    Adiciona um JSON à fila de um pedido no Google Sheets.
    
    Args:
        numero_pedido (str): Número do pedido
        json_data (dict): JSON a ser adicionado na fila
        web_app_url (str): URL do Web App do Google Apps Script
        engine: Engine object para logging
    
    Returns:
        dict: Resultado da operação
    """
    debug = f"push_fila: inicio numero_pedido={numero_pedido} - "
    try:
        payload = {
            "acao": "push",
            "numeroPedido": str(numero_pedido),
            "json": json_data
        }
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        debug += "fazendo_requisicao_push - "
        # Permitir redirecionamentos (Google Apps Script pode fazer redirects)
        response = requests.post(web_app_url, json=payload, headers=headers, timeout=30, allow_redirects=True)
        
        # Verificar se a resposta tem conteúdo
        response_text = response.text.strip()
        
        if not response_text:
            debug += "resposta_vazia - "
            return {
                "status": "erro",
                "mensagem": "Resposta vazia do Google Apps Script",
                "numeroPedido": numero_pedido,
                "debug": debug
            }
        
        try:
            result = response.json()
            debug += f"resposta_parseada status={result.get('status')} - "
        except json.JSONDecodeError as json_err:
            debug += f"erro_parse_json={str(json_err)} - "
            return {
                "status": "erro",
                "mensagem": f"Resposta inválida do Google Apps Script: {str(json_err)}",
                "numeroPedido": numero_pedido,
                "resposta_raw": response_text,
                "debug": debug
            }
        
        response.raise_for_status()
        
        if result.get("status") == "ok":
            debug += "push_sucesso - "
            return {
                "status": "ok",
                "mensagem": "JSON adicionado à fila com sucesso",
                "numeroPedido": numero_pedido,
                "debug": debug
            }
        else:
            debug += f"push_erro mensagem={result.get('mensagem', 'N/A')} - "
            return {
                "status": "erro",
                "mensagem": result.get("mensagem", "Erro ao adicionar JSON na fila"),
                "numeroPedido": numero_pedido,
                "debug": debug
            }
            
    except requests.exceptions.RequestException as e:
        debug += f"erro_requisicao={str(e)} - "
        return {
            "status": "erro",
            "mensagem": f"Erro na requisição: {str(e)}",
            "numeroPedido": numero_pedido,
            "debug": debug
        }
    except Exception as e:
        debug += f"erro_inesperado={str(e)} - "
        return {
            "status": "erro",
            "mensagem": f"Erro inesperado: {str(e)}",
            "numeroPedido": numero_pedido,
            "debug": debug
        }


def get_fila(numero_pedido, web_app_url, engine):
    """
    Retorna o primeiro JSON da fila de um pedido específico sem remover nada.
    
    Args:
        numero_pedido (str): Número do pedido
        web_app_url (str): URL do Web App do Google Apps Script
        engine: Engine object para logging
    
    Returns:
        dict: Resultado da operação com o primeiro JSON do pedido (se houver)
    """
    debug = f"get_fila: inicio numero_pedido={numero_pedido} - "
    try:
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        # Adicionar numeroPedido como query parameter
        url_with_params = f"{web_app_url}?numeroPedido={numero_pedido}"
        debug += f"url={url_with_params} - "
        
        debug += "fazendo_requisicao_get - "
        # Fazer requisição GET com query parameter
        response = requests.get(url_with_params, headers=headers, timeout=30, allow_redirects=True)
        
        # Verificar se a resposta tem conteúdo
        response_text = response.text.strip()
        
        if not response_text:
            debug += "resposta_vazia - "
            return {
                "status": "erro",
                "mensagem": "Resposta vazia do Google Apps Script",
                "debug": debug
            }
        
        try:
            result = response.json()
            debug += f"resposta_parseada status={result.get('status')} - "
        except json.JSONDecodeError as json_err:
            debug += f"erro_parse_json={str(json_err)} - "
            return {
                "status": "erro",
                "mensagem": f"Resposta inválida do Google Apps Script: {str(json_err)}",
                "resposta_raw": response_text[:200] if len(response_text) > 200 else response_text,
                "debug": debug
            }
        
        response.raise_for_status()
        
        if result.get("status") == "ok":
            primeiro_json = result.get("primeiroJson")
            debug += f"primeiro_json_existe={primeiro_json is not None} - "
            return {
                "status": "ok",
                "mensagem": "Primeiro JSON da fila retornado com sucesso",
                "numeroPedido": numero_pedido,
                "primeiroJson": primeiro_json,
                "debug": debug
            }
        elif result.get("status") == "fila_vazia":
            debug += "fila_vazia - "
            return {
                "status": "fila_vazia",
                "mensagem": "Fila do pedido existe, mas está vazia",
                "numeroPedido": numero_pedido,
                "primeiroJson": None,
                "debug": debug
            }
        elif result.get("status") == "nao_encontrado":
            debug += "nao_encontrado - "
            return {
                "status": "nao_encontrado",
                "mensagem": "Pedido não encontrado na fila",
                "numeroPedido": numero_pedido,
                "primeiroJson": None,
                "debug": debug
            }
        elif result.get("status") == "vazia":
            debug += "vazia - "
            return {
                "status": "vazia",
                "mensagem": "Nenhum pedido na fila",
                "numeroPedido": numero_pedido,
                "primeiroJson": None,
                "debug": debug
            }
        else:
            debug += f"status_inesperado={result.get('status')} - "
            return {
                "status": "erro",
                "mensagem": result.get("mensagem", "Erro ao obter primeiro JSON da fila"),
                "numeroPedido": numero_pedido,
                "debug": debug
            }
            
    except requests.exceptions.RequestException as e:
        debug += f"erro_requisicao={str(e)} - "
        return {
            "status": "erro",
            "mensagem": f"Erro na requisição: {str(e)}",
            "debug": debug
        }
    except Exception as e:
        debug += f"erro_inesperado={str(e)} - "
        return {
            "status": "erro",
            "mensagem": f"Erro inesperado: {str(e)}",
            "debug": debug
        }


def disparar_zaffari_substituicao(client_reference, engine, debug_context=None):
    """
    Dispara o endpoint do CodeAction zaffari_substitu_automatica após adicionar item na fila.
    
    Args:
        client_reference (str): Número do pedido (client_reference)
        engine: Engine object para logging
        debug_context (list): Lista para acumular mensagens de debug (opcional)
    
    Returns:
        bool: True se disparou com sucesso, False caso contrário
    """
    debug = f"disparar_zaffari: inicio client_reference={client_reference} - "
    try:
        codeaction_url = "https://code-actions.weni.ai/action/endpoint/68f8d2aed00e9b8c9a3d98b5"
        
        # Adicionar client_reference como query parameter
        url_with_params = f"{codeaction_url}?client_reference={client_reference}"
        debug += f"url={url_with_params} - "
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        debug += "fazendo_requisicao_post - "
        # Fazer requisição POST sem body (apenas com query parameter)
        response = requests.post(url_with_params, headers=headers, timeout=30)
        
        debug += f"status_code={response.status_code} - "
        if response.status_code == 200:
            debug += "sucesso - "
            if debug_context is not None:
                debug_context.append(debug)
            return True
        else:
            debug += f"falhou status={response.status_code} response_text={response.text[:200]} - "
            if debug_context is not None:
                debug_context.append(debug)
            return False            
    except requests.exceptions.RequestException as e:
        debug += f"erro_requisicao={str(e)} - "
        if debug_context is not None:
            debug_context.append(debug)
        return False
    except Exception as e:
        debug += f"erro_inesperado={str(e)} - "
        if debug_context is not None:
            debug_context.append(debug)
        return False


def pop_fila(numero_pedido, web_app_url, engine):
    """
    Remove e retorna o primeiro JSON da fila de um pedido no Google Sheets.
    
    Args:
        numero_pedido (str): Número do pedido
        web_app_url (str): URL do Web App do Google Apps Script
        engine: Engine object para logging
    
    Returns:
        dict: Resultado da operação com o JSON removido (se houver)
    """
    debug = "pop_fila: inicio - "
    try:
        debug += f"numero_pedido={numero_pedido} - "
        payload = {
            "acao": "pop",
            "numeroPedido": str(numero_pedido)
        }
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        debug += "fazendo_requisicao_pop - "
        response = requests.post(web_app_url, json=payload, headers=headers, timeout=30, allow_redirects=True)
        response_text = response.text.strip()
        
        if not response_text:
            debug += "resposta_vazia - "
            return {
                "status": "erro",
                "mensagem": "Resposta vazia do Google Apps Script",
                "numeroPedido": numero_pedido,
                "debug": debug
            }
        
        try:
            result = response.json()
            debug += f"resposta_parseada status={result.get('status')} - "
        except json.JSONDecodeError as json_err:
            debug += f"erro_parse_json={str(json_err)} - "
            return {
                "status": "erro",
                "mensagem": f"Resposta inválida do Google Apps Script: {str(json_err)}",
                "numeroPedido": numero_pedido,
                "resposta_raw": response_text[:200] if len(response_text) > 200 else response_text,
                "debug": debug
            }
        
        response.raise_for_status()
        
        if result.get("status") == "ok":
            json_removido = result.get("jsonRemovido")
            disparou_zaffari = False
            debug += f"pop_sucesso json_removido_existe={json_removido is not None} - "
            
            # Verificar se há próximo item na fila e disparar zaffari
            debug += "verificando_proximo_item_na_fila - "
            try:
                get_result = get_fila(numero_pedido, web_app_url, engine)
                debug += f"get_fila_status={get_result.get('status')} - "
                
                # Incluir debug do get_fila se disponível
                if "debug" in get_result:
                    debug += f"get_fila_debug={get_result.get('debug', '')} - "
                
                if get_result.get("status") == "ok":
                    primeiro_json = get_result.get("primeiroJson")
                    debug += f"proximo_item_encontrado primeiro_json_existe={primeiro_json is not None} - "
                    debug += "disparando_zaffari_substituicao - "
                    # Criar lista para capturar debug do disparo
                    disparo_debug = []
                    disparou_zaffari = disparar_zaffari_substituicao(numero_pedido, engine, debug_context=disparo_debug)
                    if disparo_debug:
                        debug += " ".join(disparo_debug) + " - "
                    debug += f"disparou_zaffari={disparou_zaffari} - "
                else:
                    debug += f"nao_ha_proximo_item status={get_result.get('status')} mensagem={get_result.get('mensagem', 'N/A')} - "
                    if get_result.get("status") == "fila_vazia":
                        debug += "fila_ficou_vazia_apos_pop - "
                    elif get_result.get("status") == "nao_encontrado":
                        debug += "pedido_nao_encontrado_apos_pop - "
                    elif get_result.get("status") == "vazia":
                        debug += "nenhum_pedido_na_fila - "
            except Exception as e:
                import traceback
                debug += f"erro_ao_verificar_proximo_item={str(e)} traceback={traceback.format_exc()[:200]} - "
            
            return {
                "status": "ok",
                "mensagem": "JSON removido da fila com sucesso",
                "numeroPedido": numero_pedido,
                "disparouZaffari": disparou_zaffari,
                "jsonRemovido": json_removido,
                "debug": debug
            }
        elif result.get("status") == "vazio_ou_inexistente":
            debug += "fila_vazia_ou_inexistente - "
            return {
                "status": "vazio_ou_inexistente",
                "mensagem": "Fila vazia ou pedido não encontrado",
                "numeroPedido": numero_pedido,
                "jsonRemovido": None,
                "debug": debug
            }
        else:
            debug += f"status_inesperado={result.get('status')} - "
            return {
                "status": "erro",
                "mensagem": result.get("mensagem", "Erro ao remover JSON da fila"),
                "numeroPedido": numero_pedido,
                "debug": debug
            }
            
    except requests.exceptions.RequestException as e:
        debug += f"erro_requisicao={str(e)} - "
        return {
            "status": "erro",
            "mensagem": f"Erro na requisição: {str(e)}",
            "numeroPedido": numero_pedido,
            "debug": debug
        }
    except Exception as e:
        debug += f"erro_inesperado={str(e)} - "
        return {
            "status": "erro",
            "mensagem": f"Erro inesperado: {str(e)}",
            "numeroPedido": numero_pedido,
            "debug": debug
        }


def Run(engine):
    """
    CodeAction para gerenciar fila no Google Sheets.
    
    IMPORTANTE: Este CodeAction requer requisições POST com body JSON.
    Requisições GET com body não são suportadas pelo HTTP e causarão erro 400.
    
    Body esperado para PUSH:
    {
        "acao": "push",
        "numeroPedido": "123",
        "json": { "foo": "bar", "valor": 10 }
    }
    
    Body esperado para POP:
    {
        "acao": "pop",
        "numeroPedido": "123"
    }
    
    Body esperado para GET:
    {
        "acao": "get",
        "numeroPedido": "123"
    }
    """
    try:
        # Validar que o body não está vazio
        body = engine.body
        
        if not body or not body.strip():
            engine.result.set({
                "status": "erro",
                "mensagem": "Body da requisição está vazio. Este CodeAction requer POST com body JSON."
            }, status_code=400, content_type="json")
            return
        
        # Tentar fazer parse do JSON
        try:
            body_dict = json.loads(body)
        except json.JSONDecodeError as json_err:
            engine.result.set({
                "status": "erro",
                "mensagem": f"Body inválido: não é um JSON válido. Erro: {str(json_err)}",
                "dica": "Certifique-se de usar POST (não GET) e enviar um JSON válido no body."
            }, status_code=400, content_type="json")
            return
        
        # Obter ação e número do pedido
        acao = body_dict.get("acao")
        # Tentar obter numeroPedido de múltiplas fontes (em ordem de prioridade):
        # 1. Query parameter (mais comum quando chamado via push_fila_from_codeaction)
        # 2. Campo numeroPedido no body (se fornecido diretamente)
        # 3. Dentro do json interno (se o json contém job.client_reference)
        numero_pedido = (
            engine.params.get("numeroPedido") or 
            body_dict.get("numeroPedido") or 
            body_dict.get("json", {}).get("job", {}).get("client_reference", "")
        )
        
        # Validações básicas
        if not acao:
            engine.result.set({
                "status": "erro",
                "mensagem": "Campo 'acao' é obrigatório. Use 'push', 'pop' ou 'get'."
            }, status_code=400, content_type="json")
            return
        
        # Todas as ações precisam de numeroPedido
        if not numero_pedido:
            # Informações de debug para ajudar a identificar o problema
            debug_info = {
                "params_numeroPedido": engine.params.get("numeroPedido") if hasattr(engine, 'params') else None,
                "body_numeroPedido": body_dict.get("numeroPedido"),
                "json_job_client_reference": body_dict.get("json", {}).get("job", {}).get("client_reference") if body_dict.get("json") else None
            }
            engine.result.set({
                "status": "erro",
                "mensagem": "Campo 'numeroPedido' é obrigatório. Não foi encontrado em query parameter, body.numeroPedido nem body.json.job.client_reference.",
                "debug": debug_info
            }, status_code=400, content_type="json")
            return
        
        # URL do Web App do Google Apps Script
        # TODO: Substituir pela URL real do seu Web App
        web_app_url = "https://script.google.com/macros/s/AKfycbz1TmE7HugI7B1rQ_e5JylHoH7nU1TIgcoMrOhyGQiRo20YoGCIQQs9GPbCKz15CLxtSQ/exec"

        
        # Processar ação
        if acao == "push":
            json_data = body_dict.get("json")
            if json_data is None:
                engine.result.set({
                    "status": "erro",
                    "mensagem": "Para acao 'push', o campo 'json' é obrigatório."
                }, status_code=400, content_type="json")
                return
            
            result = push_fila(numero_pedido, json_data, web_app_url, engine)
            status_code = 200 if result.get("status") == "ok" else 400
            engine.result.set(result, status_code=status_code, content_type="json")
            return
        elif acao == "pop":
            result = pop_fila(numero_pedido, web_app_url, engine)
            if result.get("status") == "vazio_ou_inexistente":
                status_code = 200  # Não é erro, apenas informação
            elif result.get("status") == "ok":
                status_code = 200
            else:
                status_code = 400
            engine.result.set(result, status_code=status_code, content_type="json")
            return
        elif acao == "get":
            result = get_fila(numero_pedido, web_app_url, engine)
            if result.get("status") in ["vazia", "fila_vazia", "nao_encontrado"]:
                status_code = 200  # Não é erro, apenas informação
            elif result.get("status") == "ok":
                status_code = 200
            else:
                status_code = 400
            engine.result.set(result, status_code=status_code, content_type="json")
            return
        else:
            engine.result.set({
                "status": "erro",
                "mensagem": f"Ação inválida: '{acao}'. Use 'push', 'pop' ou 'get'."
            }, status_code=400, content_type="json")
            return
            
    except json.JSONDecodeError as e:
        engine.result.set({
            "status": "erro",
            "mensagem": f"Erro ao processar JSON: {str(e)}"
        }, status_code=400, content_type="json")
        return
    except Exception as e:
        engine.result.set({
            "status": "erro",
            "mensagem": f"Erro inesperado: {str(e)}"
        }, status_code=500, content_type="json")
        return

