import json
import requests
import ast
import re
from urllib.parse import urlparse, parse_qs

#690c90d74b759e31596c7a58 - submit_replacement_choice

WENI_CONTACTS_URL = "https://flows.weni.ai/api/v2/contacts.json"
WENI_AUTH_HEADER_VALUE = "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70"
WENI_HEADERS = {
    "Authorization": WENI_AUTH_HEADER_VALUE,
    "Content-Type": "application/json",
}

# Regex para remover sequências de escape inválidas em JSON armazenado em campos da Weni
INVALID_ESCAPE_SEQUENCE_RE = re.compile(r'\\(?!(["\\/bfnrt]|u[0-9a-fA-F]{4}))')
def send_replacement_suggestion_to_zaffari(order_id, product_id_original, product_id_replacement, quantity, engine):
    """
    Envia sugestão de substituição escolhida para a API da Zaffari (ambiente QA).

    POST https://hml-api.zaffari.com.br/ecommerce/integration-matrix/api/v1/flow/orderProductReplacementSugestion
    Body:
    {
      "orderId": "string",
      "productIdOriginal": "string",
      "productIdReplacement": "string",
      "quantity": 0.01
    }
    """
    try:
        base_url = "https://hml-api.zaffari.com.br/ecommerce/integration-matrix"
        url = f"{base_url}/api/v1/flow/orderProductReplacementSugestion"
        payload = {
            "orderId": str(order_id or ""),
            "productIdOriginal": str(product_id_original or ""),
            "productIdReplacement": str(product_id_replacement or ""),
            "quantity": float(quantity or 0),
        }
        engine.log.debug(f"payload: {payload}")
        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": "5400fb5a63a945b1a2bbab6086a94c71"
        }
        engine.log.debug(f"Enviando requisição para: {url}")
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        engine.log.debug(f"resp status code: {resp.status_code}")
        engine.log.debug(f"resp text: {resp.text}")
        
        # Tentar parsear JSON da resposta, mas não falhar se não for JSON
        try:
            resp_json = resp.json()
            engine.log.debug(f"resp JSON: {resp_json}")
        except (ValueError, json.JSONDecodeError):
            engine.log.debug(f"Resposta não é JSON válido")
        
        return 200 <= resp.status_code < 300
    except requests.exceptions.RequestException as e:
        engine.log.debug(f"Erro na requisição HTTP: {type(e).__name__}: {str(e)}")
        return False
    except Exception as e:
        engine.log.debug(f"Erro inesperado: {type(e).__name__}: {str(e)}")
        return False


def _ensure_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            try:
                return ast.literal_eval(value)
            except Exception:
                return {}
    return {}


def _sanitize_body_string(body: str) -> str:
    """
    Corrige problemas comuns em strings de body semiformatadas:
    - Falta de vírgula entre 'produtos_escolhidos' e 'chosen_sku'
    - orderId sem aspas contendo hífen (ex.: 1543330509898-01)
    """
    s = body
    try:
        # Inserir vírgula faltante antes de "chosen_sku" após array de produtos
        # ...]}"chosen_sku" -> ...]},"chosen_sku"
        s = re.sub(r"\](\s*)\"chosen_sku\"", r"],\1\"chosen_sku\"", s)

        # Colocar aspas no orderId quando houver hífen e ele não estiver entre aspas
        # "orderId": 154...-01 -> "orderId": "154...-01"
        def _quote_orderid(m):
            value = m.group(2).strip()
            # já possui aspas
            if value.startswith('"') and value.endswith('"'):
                return m.group(0)
            # só quotar se contiver hífen
            if '-' in value:
                return f"{m.group(1)}\"{value}\""
            return m.group(0)

        s = re.sub(r'(\"orderId\"\s*:\s*)([^,\n\r}]+)', _quote_orderid, s)

        # Remover barras invertidas inválidas antes de caracteres não suportados por JSON
        # Mantém apenas escapes válidos: \" \\ \/ \b \f \n \r \t \u
        s = re.sub(r'\\(?!["\\/bfnrtu])', '', s)
    except Exception:
        pass
    return s


def _log_debug(engine, message, data=None):
    try:
        if hasattr(engine, "log") and hasattr(engine.log, "debug"):
            if data is not None:
                try:
                    engine.log.debug(f"{message}: {json.dumps(data)[:500]}")
                except Exception:
                    engine.log.debug(f"{message}: {str(data)[:500]}")
            else:
                engine.log.debug(message)
    except Exception:
        pass


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
        results = data.get("results") or {}
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


def _load_produtos_from_weni(merged: dict, urn: str, teste: str = ""):
    """
    Lê os produtos salvos nas variáveis de contato da Weni (items_partN).
    Usa o telefone/URN presente no payload (ex.: customer_phone, phone, to).
    Retorna (lista_de_produtos, teste_atualizado).
    """
    teste_local = teste if isinstance(teste, str) else ""

    # Tentar obter telefone ou URN do payload
    phone = urn
    if not phone:
        teste_local += "load_weni: phone_ou_urn_nao_encontrado - "
        return [], teste_local

    #urn = _format_phone_to_urn(str(phone))
    teste_local += f"load_weni: urn={urn} - "

    fields, teste_local = _fetch_weni_contact_fields(urn, teste)
    if not isinstance(fields, dict) or not fields:
        teste_local += "load_weni: nenhum_field_encontrado - "
        return [], teste_local

    raw_produtos = []
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
                    raw_produtos.extend(items)
                    teste_local += f"load_weni:{key}_itens_dict={len(items)} - "
            elif isinstance(parsed, list):
                raw_produtos.extend(parsed)
                teste_local += f"load_weni:{key}_itens_list={len(parsed)} - "
        elif isinstance(raw_chunk, dict):
            items = raw_chunk.get("items")
            if isinstance(items, list):
                raw_produtos.extend(items)
                teste_local += f"load_weni:{key}_itens_dict_direto={len(items)} - "

    if not raw_produtos:
        teste_local += "load_weni: nenhum_produto_encontrado - "
    else:
        teste_local += f"load_weni: total_produtos={len(raw_produtos)} - "

    return raw_produtos, teste_local


def _best_effort_parse_str(raw: str):
    """
    Tenta diversas estratégias para transformar a string em dict.
    Retorna (dict, metodo_usado)
    """
    # 1) Tentar JSON direto
    try:
        return json.loads(raw), 'json.loads(raw)'
    except Exception:
        pass

    # 2) Sanitizar e tentar JSON novamente
    sanitized = _sanitize_body_string(raw)
    try:
        return json.loads(sanitized), 'json.loads(sanitized)'
    except Exception:
        pass

    # 3) Remover barras inválidas de novo (dupla salvaguarda) e tentar ast
    sanitized2 = re.sub(r'\\(?!["\\/bfnrtu])', '', sanitized)
    try:
        return ast.literal_eval(sanitized2), 'ast.literal_eval(sanitized2)'
    except Exception:
        pass

    # 4) Extração manual via regex (best-effort)
    result = {}
    try:
        # orderId
        m = re.search(r'"orderId"\s*:\s*("?)([^",\n\r}]+)\1', sanitized2)
        if m:
            result['orderId'] = m.group(2)

        # productIdOriginal
        m = re.search(r'"productIdOriginal"\s*:\s*("?)([^",\n\r}]+)\1', sanitized2)
        if m:
            result['productIdOriginal'] = m.group(2)

        # chosen_sku
        m = re.search(r'"chosen_sku"\s*:\s*("?)([^",\n\r}]+)\1', sanitized2)
        if m:
            result['chosen_sku'] = m.group(2)

        # produtos_escolhidos (pega o bloco entre colchetes)
        m = re.search(r'"produtos_escolhidos"\s*:\s*(\[[\s\S]*?\])', sanitized2)
        if m:
            produtos_str = m.group(1)
            # tentar como JSON
            try:
                result['produtos_escolhidos'] = json.loads(produtos_str)
            except Exception:
                # remover barras inválidas e tentar ast
                produtos_str2 = re.sub(r'\\(?!["\\/bfnrtu])', '', produtos_str)
                try:
                    result['produtos_escolhidos'] = ast.literal_eval(produtos_str2)
                except Exception:
                    pass
    except Exception:
        pass

    return result, 'regex-extract'


def _extract_input_fields(input_data, produtos_escolhidos=None):
    """
    Extrai os campos necessários a partir do JSON de entrada.
    Suporta dois formatos:
      A) Campos diretos: orderId, productIdOriginal, productIdReplacement, quantity
      B) Lista de produtos + seleção: produtos_escolhidos + chosen_sku (ou chosen_index)
    """
    # Normalizar orderId (pode vir como string ou número)
    order_id = input_data.get("orderId") or input_data.get("order_id")
    if order_id is not None:
        order_id = str(order_id)
    
    # Normalizar productIdOriginal (pode vir como string ou número)
    product_id_original = (
        input_data.get("productIdOriginal")
        or input_data.get("product_id_original")
        or input_data.get("produto_original")
        or input_data.get("sku_original")
    )
    if product_id_original is not None:
        product_id_original = str(product_id_original)
    
    # Normalizar productIdReplacement (pode vir como string ou número)
    product_id_replacement = (
        input_data.get("productIdReplacement")
        or input_data.get("product_id_replacement")
        or input_data.get("sku_replacement")
    )
    if product_id_replacement is not None:
        product_id_replacement = str(product_id_replacement)
    
    quantity = input_data.get("quantity")
    if quantity is not None:
        try:
            quantity = float(quantity)
        except (ValueError, TypeError):
            quantity = None

    # Se não veio direto, tentar derivar de produtos_escolhidos (ex.: vindos da Weni) + chosen_sku/index
    if (not product_id_replacement or quantity is None) and produtos_escolhidos:
        produtos = produtos_escolhidos or []
        chosen_sku = (
            input_data.get("productIdReplacement")
            or input_data.get("chosen_sku")
            or input_data.get("sku_replacement")
        )
        chosen_index = input_data.get("chosen_index")

        selected = None
        if chosen_sku is not None:
            # Normalizar chosen_sku para string para comparação
            chosen_sku_str = str(chosen_sku)
            for p in produtos:
                # Normalizar sku do produto para string para comparação
                p_sku = str(p.get("sku", ""))
                if p_sku == chosen_sku_str:
                    selected = p
                    break
        elif chosen_index is not None:
            try:
                chosen_index = int(chosen_index)
                if 0 <= chosen_index < len(produtos):
                    selected = produtos[chosen_index]
            except (ValueError, TypeError):
                pass

        if selected:
            product_id_replacement = str(selected.get("sku", ""))
            # quantity: se vier no item, usa; senão, fallback 1
            q = selected.get("quantity")
            if q is not None:
                try:
                    quantity = float(q)
                except (ValueError, TypeError):
                    quantity = 1.0
            else:
                quantity = 1.0

    return order_id, product_id_original, product_id_replacement, quantity


def Run(engine):
    teste = "inicio - "
    try:
        body = engine.body
        _log_debug(engine, "submit_replacement_choice - raw body type", str(type(body)))
        try:
            _log_debug(engine, "submit_replacement_choice - raw body preview", str(body)[:500])
        except Exception:
            pass

        # Se já for dict, usar direto
        if isinstance(body, dict):
            body_dict = body
        # Se for string, tentar parsear como JSON
        elif isinstance(body, str):
            raw = body
            body_dict, method = _best_effort_parse_str(raw)
            _log_debug(engine, "parsed body via", method)
            _log_debug(engine, "parsed body content", body_dict)
        else:
            # Se for None ou outro tipo, usar dict vazio
            body_dict = body if body else {}
            
    except Exception as e:
        teste += f"body_invalido_erro={e} - "
        engine.result.set({
            "error": "Body inválido (não é JSON)",
            "details": str(e),
            "body_type": str(type(engine.body)),
            "body_preview": str(engine.body)[:200] if engine.body else None,
            "teste": teste,
        }, status_code=400, content_type="json")
        return

    # Alguns provedores enviam os dados dentro de 'params' ou 'payload'
    merged = dict(body_dict or {})
    try:
        merged_keys = list(merged.keys())
    except Exception:
        merged_keys = []
    teste += f"merged_keys_iniciais={merged_keys} - "
    _log_debug(engine, "initial keys", list(merged.keys()))
    if isinstance(merged.get("params"), (dict, str)):
        params_dict = _ensure_dict(merged.get("params"))
        if params_dict:
            merged.update(params_dict)
            _log_debug(engine, "merged params keys", list(params_dict.keys()))
    if isinstance(merged.get("payload"), (dict, str)):
        payload_dict = _ensure_dict(merged.get("payload"))
        if payload_dict:
            merged.update(payload_dict)
            _log_debug(engine, "merged payload keys", list(payload_dict.keys()))

    # Também tentar ler parâmetros enviados via querystring/engine
    try:
        if hasattr(engine, "params") and isinstance(engine.params, dict):
            merged.update(engine.params)
            _log_debug(engine, "merged engine.params keys", list(engine.params.keys()))
    except Exception:
        pass
    try:
        if hasattr(engine, "query") and isinstance(engine.query, dict):
            merged.update(engine.query)
            _log_debug(engine, "merged engine.query keys", list(engine.query.keys()))
    except Exception:
        pass
    try:
        if hasattr(engine, "query_params") and isinstance(engine.query_params, dict):
            merged.update(engine.query_params)
            _log_debug(engine, "merged engine.query_params keys", list(engine.query_params.keys()))
    except Exception:
        pass

    # Fallback robusto: tentar extrair a query diretamente de possíveis URLs do engine
    try:
        candidate_attrs = [
            "url",
            "request_url",
            "original_url",
            "raw_url",
            "path",
            "full_path",
        ]
        extracted = {}
        for attr in candidate_attrs:
            try:
                val = getattr(engine, attr, None)
                if isinstance(val, str) and "?" in val:
                    parsed = urlparse(val)
                    if parsed.query:
                        qs = parse_qs(parsed.query)
                        # pega o primeiro valor de cada chave
                        for k, v in qs.items():
                            if isinstance(v, list) and v:
                                extracted[k] = v[0]
                            elif isinstance(v, str):
                                extracted[k] = v
            except Exception:
                continue
        if extracted:
            merged.update(extracted)
            _log_debug(engine, "merged extracted query from url attrs", list(extracted.keys()))
    except Exception:
        pass

    # Carregar produtos salvos na Weni (caso B)
    urn = engine.params.get("urn")
    produtos_weni, teste = _load_produtos_from_weni(merged, urn, teste)
    teste += f"produtos_weni_count={len(produtos_weni)} - "

    order_id, product_id_original, product_id_replacement, quantity = _extract_input_fields(
        merged, produtos_escolhidos=produtos_weni
    )
    _log_debug(engine, "extracted fields", {
        "orderId": order_id,
        "productIdOriginal": product_id_original,
        "productIdReplacement": product_id_replacement,
        "quantity": quantity,
    })
    teste += f"extracted_fields orderId={order_id} original={product_id_original} replacement={product_id_replacement} quantity={quantity} - "

    # Validações mínimas
    missing = []
    if not order_id:
        missing.append("orderId")
    if not product_id_original:
        missing.append("productIdOriginal")
    if not product_id_replacement:
        missing.append("productIdReplacement")
    if quantity is None:
        missing.append("quantity")

    if missing:
        _log_debug(engine, "missing required fields", missing)
        teste += f"missing_campos={missing} - "
        engine.result.set({
            "error": "Parâmetros obrigatórios ausentes",
            "missing": missing,
            "received_keys": list((merged or {}).keys()),
            "teste": teste,
        }, status_code=400, content_type="json")
        return

    ok = send_replacement_suggestion_to_zaffari(
        order_id=order_id,
        product_id_original=product_id_original,
        product_id_replacement=product_id_replacement,
        quantity=quantity,
        engine=engine,
    )

    if ok:
        teste += "envio_zaffari_status=sucesso - "
        engine.result.set({
            "Status": "Success",
            "orderId": str(order_id),
            "productIdOriginal": str(product_id_original),
            "productIdReplacement": str(product_id_replacement),
            "quantity": float(quantity),
            "teste": teste,
        }, status_code=200, content_type="json")
    else:
        teste += "envio_zaffari_status=erro - "
        engine.result.set({
            "Status": "Error",
            "orderId": str(order_id),
            "productIdOriginal": str(product_id_original),
            "productIdReplacement": str(product_id_replacement),
            "quantity": float(quantity),
            "teste": teste,
        }, status_code=502, content_type="json")


