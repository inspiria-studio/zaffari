import json
import requests
import re

# CodeAction para obter o nome do item escolhido baseado no ID/SKU - get_chosen_item_name - 693181fb7c9236f6f6411aca

# Configuração da Weni (mesmo projeto utilizado em zaffari_substitu_automatica / submit_replacement_choice)
WENI_CONTACTS_URL = "https://flows.weni.ai/api/v2/contacts.json"
WENI_AUTH_HEADER_VALUE = "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70"
WENI_HEADERS = {
    "Authorization": WENI_AUTH_HEADER_VALUE,
    "Content-Type": "application/json",
}

# Regex para remover sequências de escape inválidas em JSON armazenado em campos da Weni
INVALID_ESCAPE_SEQUENCE_RE = re.compile(r'\\(?!(["\\/bfnrt]|u[0-9a-fA-F]{4}))')

def get_item_name_by_id(items, chosen_id):
    """
    Busca o nome do item na lista de sugestões baseado no ID/SKU escolhido.
    
    Args:
        items (list): Lista de itens de sugestão no formato:
            [{"sku": "1038070", "name": "...", "price": 1.99, "quantity": 2, "image_url": "..."}]
        chosen_id (str): ID/SKU do item escolhido
    
    Returns:
        str: Nome do item escolhido ou None se não encontrado
    """
    if not items or not isinstance(items, list):
        return None
    
    if not chosen_id:
        return None
    
    # Normalizar chosen_id para string para comparação
    chosen_id_str = str(chosen_id).strip()
    
    # Buscar o item na lista pelo SKU
    for item in items:
        if not isinstance(item, dict):
            continue
        
        # Tentar buscar pelo campo "sku"
        item_sku = item.get("sku")
        if item_sku:
            item_sku_str = str(item_sku).strip()
            if item_sku_str == chosen_id_str:
                # Retornar o nome do item encontrado
                return item.get("name", "")
        
        # Fallback: tentar buscar por "id" se não tiver "sku"
        item_id = item.get("id")
        if item_id:
            item_id_str = str(item_id).strip()
            if item_id_str == chosen_id_str:
                return item.get("name", "")
    
    return None


def _sanitize_weni_field(raw_text: str, teste: str = ""):
    """
    Remove barras invertidas inválidas de strings JSON vindas da Weni
    (ex.: \"Mo\ída\" -> \"Moída\"), para permitir json.loads sem erro.
    Também adiciona informações na string de log `teste`.
    """
    if not isinstance(raw_text, str) or "\\" not in raw_text:
        return raw_text, teste

    sanitized = raw_text
    total_replacements = 0

    while True:
        def _replace(match):
            nonlocal total_replacements
            total_replacements += 1
            return ""

        new_sanitized = INVALID_ESCAPE_SEQUENCE_RE.sub(_replace, sanitized)
        if new_sanitized == sanitized:
            break
        sanitized = new_sanitized

    if total_replacements:
        teste += f"sanitize_weni_field: removed_escapes={total_replacements} - "

    return sanitized, teste


def _format_phone_to_urn(phone: str) -> str:
    """
    Formata telefone em formato de URN para Weni (whatsapp:55XXXXXXXXXXX).
    Aceita tanto números com/sem +55 quanto já no formato whatsapp:.
    """
    if not isinstance(phone, str):
        return ""
    phone = phone.strip()
    if phone.startswith("whatsapp:"):
        return phone
    digits = re.sub(r"[^\d]", "", phone)
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("55"):
        digits = digits[2:]
    if len(digits) == 10:
        ddd, numero = digits[:2], digits[2:]
        if not numero.startswith("9"):
            numero = "9" + numero
        digits = ddd + numero
    return f"whatsapp:55{digits}"


def _fetch_weni_contact_fields(urn: str, teste: str = ""):
    """
    Busca o contato na Weni pelo URN e retorna (fields_dict, teste_atualizado).
    """
    if not urn:
        teste += "fetch_weni: urn_vazio - "
        return {}, teste
    try:
        resp = requests.get(WENI_CONTACTS_URL, headers=WENI_HEADERS, params={"urn": urn}, timeout=15)
        status = resp.status_code
        teste += f"fetch_weni: status={status} - "
        if not (200 <= status < 300):
            teste += f"fetch_weni: http_error_status={status} - "
            return {}, teste
        data = resp.json()
        results = data.get("results") or []
        teste += f"fetch_weni: results_len={len(results)} - "
        if not results:
            return {}, teste
        contact = results[0] or {}
        fields = contact.get("fields") or {}
        field_keys = list(fields.keys())
        teste += f"fetch_weni: field_keys={field_keys} - "
        return fields, teste
    except Exception as e:
        teste += f"fetch_weni: unexpected_error={e} - "
        return {}, teste


def _load_items_from_weni(urn: str, teste: str = ""):
    """
    Lê os produtos/substitutos salvos em items_partN nas variáveis de contato da Weni.
    Retorna (lista_de_itens, teste_atualizado).
    """
    teste_local = teste if isinstance(teste, str) else ""

    if not urn:
        teste_local += "load_weni: urn_nao_informado - "
        return [], teste_local

    # Normalizar qualquer telefone cru para URN
    urn_norm = _format_phone_to_urn(urn)
    teste_local += f"load_weni: urn={urn_norm} - "

    fields, teste_local = _fetch_weni_contact_fields(urn_norm, teste_local)
    if not isinstance(fields, dict) or not fields:
        teste_local += "load_weni: nenhum_field_encontrado - "
        return [], teste_local

    raw_items = []
    chunk_keys = sorted(k for k in fields.keys() if str(k).startswith("items_part"))
    teste_local += f"load_weni: chunk_keys={chunk_keys} - "

    for key in chunk_keys:
        raw_chunk = fields.get(key)
        if isinstance(raw_chunk, str) and raw_chunk.strip():
            sanitized, teste_local = _sanitize_weni_field(raw_chunk, teste_local)
            try:
                parsed = json.loads(sanitized)
            except Exception as e:
                teste_local += f"load_weni: erro_parse_{key}={e} - "
                continue
            if isinstance(parsed, dict):
                items = parsed.get("items")
                if isinstance(items, list):
                    raw_items.extend(items)
                    teste_local += f"load_weni:{key}_itens_dict={len(items)} - "
            elif isinstance(parsed, list):
                raw_items.extend(parsed)
                teste_local += f"load_weni:{key}_itens_list={len(parsed)} - "
        elif isinstance(raw_chunk, dict):
            items = raw_chunk.get("items")
            if isinstance(items, list):
                raw_items.extend(items)
                teste_local += f"load_weni:{key}_itens_dict_direto={len(items)} - "

    if not raw_items:
        teste_local += "load_weni: nenhum_item_encontrado - "
    else:
        teste_local += f"load_weni: total_itens={len(raw_items)} - "

    return raw_items, teste_local


def Run(engine):
    """
    CodeAction para obter o nome do item escolhido.
    
    Parâmetros esperados (via body ou params):
    {
        "items": [
            {
                "sku": "1038070",
                "name": "Água Mineral com Gás Água da Pedra 500ml",
                "price": 1.99,
                "quantity": 2,
                "image_url": "https://..."
            }
        ],
        "chosen_id": "1038070"
    }
    
    Ou via query string:
    ?items=[...]&chosen_id=1038070
    
    Retorna:
    {
        "status": "success",
        "item_name": "Nome do item escolhido"
    }
    
    Ou em caso de erro:
    {
        "status": "error",
        "error": "Mensagem de erro"
    }
    """
    teste = "inicio - "
    try:
        # Extrair ID do item escolhido somente dos params
        params = getattr(engine, "params", {}) or {}
        chosen_id = (
            params.get("chosen_id")
            or params.get("chosenId")
            or params.get("item_id")
            or params.get("sku")
            or params.get("id")
        )
        urn_param = (
            params.get("urn")
            or params.get("customer_phone")
            or params.get("phone")
        )
        teste += f"params_keys={list(params.keys()) if isinstance(params, dict) else []} - "
        teste += f"chosen_id={chosen_id} - urn_param={urn_param} - "

        if not chosen_id:
            teste += "erro_sem_chosen_id - "
            engine.result.set({
                "status": "error",
                "error": "Campo 'chosen_id' é obrigatório",
                "teste": teste,
            }, status_code=400, content_type="json")
            return

        if not urn_param:
            teste += "erro_sem_urn - "
            engine.result.set({
                "status": "error",
                "error": "Campo 'urn' (ou phone/customer_phone) é obrigatório para buscar os itens na Weni",
                "teste": teste,
            }, status_code=400, content_type="json")
            return

        # Buscar itens em items_partN na Weni
        items, teste = _load_items_from_weni(str(urn_param), teste)

        if not items:
            teste += "erro_sem_itens_weni - "
            engine.result.set({
                "status": "error",
                "error": "Nenhum item de substituição encontrado nas variáveis de contato da Weni",
                "chosen_id": str(chosen_id),
                "teste": teste,
            }, status_code=404, content_type="json")
            return

        if not isinstance(items, list):
            teste += f"itens_weni_tipo_invalido={type(items)} - "
            engine.result.set({
                "status": "error",
                "error": "Estrutura de 'items' na Weni não é uma lista/array",
                "teste": teste,
            }, status_code=500, content_type="json")
            return

        # Buscar o nome do item
        item_name = get_item_name_by_id(items, chosen_id)

        if item_name is None:
            teste += "item_nao_encontrado_nos_itens_weni - "
            engine.result.set({
                "status": "error",
                "error": f"Item com ID/SKU '{chosen_id}' não encontrado na lista de sugestões da Weni",
                "chosen_id": str(chosen_id),
                "teste": teste,
            }, status_code=404, content_type="json")
            return

        teste += "sucesso_item_encontrado - "
        # Retornar sucesso com o nome do item
        engine.result.set({
            "status": "success",
            "item_name": item_name,
            "chosen_id": str(chosen_id),
            "teste": teste,
        }, status_code=200, content_type="json")
        return

    except json.JSONDecodeError as e:
        teste += f"json_error={e} - "
        engine.result.set({
            "status": "error",
            "error": f"Erro ao processar JSON: {str(e)}",
            "teste": teste,
        }, status_code=400, content_type="json")
        return
    except Exception as e:
        teste += f"unexpected_error={e} - "
        engine.result.set({
            "status": "error",
            "error": f"Erro inesperado: {str(e)}",
            "teste": teste,
        }, status_code=500, content_type="json")
        return

