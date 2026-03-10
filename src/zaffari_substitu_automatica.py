import requests
import json
import sys
import re
import base64
import httpx
import time

# Imports do Google removidos - não são necessários para o funcionamento
#68f8d2aed00e9b8c9a3d98b5 - zaffari_substitu_automatica

#"produtos_escolhidos": @results.produtos_escolhidos,
# Regex para sanitizar sequências de escape inválidas em JSON armazenado em campos da Weni
INVALID_ESCAPE_SEQUENCE_RE = re.compile(r'\\(?!(["\\/bfnrt]|u[0-9a-fA-F]{4}))')


def make_request_with_retry(method, url, max_retries=2, initial_delay=0.5, **kwargs):
    """
    Faz uma requisição HTTP com retry e backoff exponencial.
    
    Args:
        method: Método HTTP ('get', 'post', etc.)
        url: URL da requisição
        max_retries: Número máximo de tentativas (padrão: 2)
        initial_delay: Delay inicial em segundos (padrão: 0.5)
        **kwargs: Argumentos adicionais para requests (timeout, headers, json, etc.)
    
    Returns:
        Response object ou None se todas as tentativas falharem
    """
    request_func = getattr(requests, method.lower(), None)
    if not request_func:
        return None
    
    delay = initial_delay
    
    for attempt in range(max_retries + 1):
        try:
            response = request_func(url, **kwargs)
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries:
                time.sleep(delay)
                delay *= 2  # Backoff exponencial
            else:
                break
        except requests.exceptions.RequestException as e:
            # Para outros erros, não fazer retry
            return None
    
    return None


def sanitize_invalid_json_escapes(raw_text: str) -> str:
    """
    Remove barras invertidas que formam sequências de escape inválidas em strings JSON.
    Útil para campos grandes salvos na Weni (ex: items_part1, items_part2, ...),
    evitando json.JSONDecodeError ao fazer json.loads.
    """
    if not raw_text or "\\" not in raw_text:
        return raw_text

    sanitized = raw_text
    total_replacements = 0

    while True:
        def _replace():
            nonlocal total_replacements
            total_replacements += 1
            return ""

        new_sanitized = INVALID_ESCAPE_SEQUENCE_RE.sub(_replace, sanitized)
        if new_sanitized == sanitized:
            break
        sanitized = new_sanitized

    return sanitized


def intelligent_search(product_name, url):
    """
    Searches for products by name and collects detailed information, determining seller ID if necessary.

    Args:
        product_name (str): Name of the product to search for
        url (str): Base URL for the search

    Returns:
        dict: Dictionary with product names as keys and their details including all variations
    """
    products_structured = {}

    search_url = f"{url}?query={product_name}&hideUnavailableItems=true"

    try:
        response = requests.get(search_url)
        response.raise_for_status()
        response_data = response.json()
        products = response_data.get("products", [])


        for product in products:
            if not product.get("items"):
                continue

            product_name_vtex = product.get("productName", "")

            # Capturar todas as variações (items) do produto
            variations = []
            for item in product.get("items", []):
                sku_id = item.get("itemId")
                sku_name = item.get("nameComplete")
                variation_item = item.get("variations", [])

                # Capturar preço do produto
                sellers = item.get("sellers", [])
                price = 0
                if sellers:
                    # Pegar o preço do primeiro vendedor disponível
                    price = sellers[0].get("commertialOffer", {}).get("Price", 0)

                if sku_id:
                    variation = {
                        "sku_id": sku_id,
                        "sku_name": sku_name,
                        "variations": variation_item,
                        "price": price,  # Adicionar preço à variação
                    }
                    variations.append(variation)

            if variations:  # Só adiciona se tiver pelo menos uma variação válida
                # Limitar variações para reduzir tamanho
                limited_variations = variations[:3]  # Máximo 3 variações por produto

                # Truncar descrição se muito longa
                description = product.get("description", "")
                if len(description) > 200:
                    description = description[:200] + "..."

                # Simplificar specification_groups (remover ou limitar drasticamente)
                spec_groups = product.get("specificationGroups", [])
                simplified_specs = []
                for group in spec_groups[:2]:  # Máximo 2 grupos
                    if group.get("specifications"):
                        # Pegar apenas as primeiras 2 especificações de cada grupo
                        limited_specs = group["specifications"][:2]
                        simplified_group = {
                            "name": group.get("name", ""),
                            "specifications": [],
                        }
                        for spec in limited_specs:
                            simplified_group["specifications"].append(
                                {
                                    "name": spec.get("name", ""),
                                    "values": spec.get("values", [])[:2],  # Máximo 2 valores
                                }
                            )
                        simplified_specs.append(simplified_group)

                # Estruturar conforme solicitado
                product_structured = {
                    "variations": limited_variations,
                    "description": description,
                    "brand": product.get("brand", ""),
                    "specification_groups": simplified_specs,  # Versão simplificada
                }

                products_structured[product_name_vtex] = product_structured

    except requests.exceptions.RequestException:
        pass
    except json.JSONDecodeError:
        pass

    return products_structured


def filter_products_with_stock(products_structured, products_with_stock):
    """
    Filtra os produtos estruturados para manter apenas aqueles que têm estoque.

    Args:
        products_structured (dict): Estrutura de produtos com variações
        products_with_stock (list): Lista de produtos que passaram na simulação do carrinho

    Returns:
        dict: Estrutura de produtos filtrada apenas com produtos que têm estoque
    """
    if not products_with_stock:
        return {}

    # Criar set dos sku_ids que têm estoque para busca rápida
    sku_ids_with_stock = {product.get("sku_id") for product in products_with_stock}

    # Filtrar a estrutura de produtos
    filtered_products = {}
    for product_name_vtex, product_data in products_structured.items():
        # Filtrar apenas as variações que têm estoque
        filtered_variations = []
        for variation in product_data["variations"]:
            if variation.get("sku_id") in sku_ids_with_stock:
                filtered_variations.append(variation)

        # Só incluir o produto se ele tiver pelo menos uma variação com estoque
        if filtered_variations:
            filtered_product_data = product_data.copy()
            filtered_product_data["variations"] = filtered_variations
            filtered_products[product_name_vtex] = filtered_product_data

    return filtered_products


def reduce_response_size(all_products_found, target_kb=90):
    """
    Reduz o tamanho da resposta removendo produtos ou campos até atingir o tamanho desejado.

    Args:
        all_products_found (dict): Dados completos dos produtos
        target_kb (int): Tamanho alvo em KB

    Returns:
        dict: Dados reduzidos
    """
    reduced_data = {}

    for product_key, product_data in all_products_found.items():
        reduced_product_data = {}

        for product_name, product_info in product_data.items():
            # Reduzir drasticamente os dados
            reduced_product_info = {
                "variations": product_info.get("variations", [])[:2],  # Máximo 2 variações
                "description": (
                    product_info.get("description", "")[:100] + "..."
                    if len(product_info.get("description", "")) > 100
                    else product_info.get("description", "")
                ),
                "brand": product_info.get("brand", ""),
                "specification_groups": [],
            }

            # Simplificar variações
            for variation in reduced_product_info["variations"]:
                if "variations" in variation:
                    variation["variations"] = variation["variations"][:1]  # Máximo 1 sub-variação

            reduced_product_data[product_name] = reduced_product_info

            temp_json = json.dumps({product_key: reduced_product_data})
            current_kb = sys.getsizeof(temp_json) / 1024

            if current_kb > target_kb:
                break

        reduced_data[product_key] = reduced_product_data

        total_json = json.dumps(reduced_data)
        total_kb = sys.getsizeof(total_json) / 1024

        if total_kb > target_kb:
            break

    return reduced_data


def build_products_details_list(products_structured):
    """
    Converte a estrutura de produtos (dict) em lista para uso na simulação de carrinho.
    """
    products_details = []

    if not isinstance(products_structured, dict):
        return products_structured or []

    for product_name, product_data in products_structured.items():
        variations = product_data.get("variations", [])
        for variation in variations:
            sku_id = variation.get("sku_id")
            if not sku_id:
                continue

            products_details.append(
                {
                    "sku_id": str(sku_id),
                    "sku_name": variation.get("sku_name", product_name),
                    "original_price": variation.get("price", 0),
                    "price": variation.get("price", 0),
                    "brand": product_data.get("brand", ""),
                    "description": product_data.get("description", ""),
                    "specification_groups": product_data.get("specification_groups", []),
                    "image_url": variation.get("image_url"),
                    "variations": variation.get("variations", []),
                }
            )

    return products_details


def convert_quantity_to_units(quantity_kg, unit_multiplier, measurement_unit):
    """
    Converte quantidade de kg para unidades quando necessário.
    Limita a 3 casas decimais.
    
    Args:
        quantity_kg (float): Quantidade em kg
        unit_multiplier (float): Peso por unidade (ex: 0.2 = cada unidade tem 0.2 kg)
        measurement_unit (str): Unidade de medida ("kg" ou "un")
    
    Returns:
        float: Quantidade convertida para unidades se necessário, senão retorna a quantidade original
                Limitado a 3 casas decimais
    """
    if measurement_unit.lower() == "kg" and unit_multiplier > 0 and unit_multiplier != 1.0:
        return round(quantity_kg / unit_multiplier, 3)
    return round(quantity_kg, 3) if isinstance(quantity_kg, float) else quantity_kg


def select_closest_products(products_structured, original_price, max_products, teste=None):
    """
    Seleciona os produtos com preço mais próximo do produto original.
    """
    if not isinstance(products_structured, dict):
        return []

    try:
        original_price = float(original_price)
        teste += " original_price: " + str(original_price) + " - "
    except (TypeError, ValueError):
        raise ValueError(f"Erro ao converter preço original para float: {original_price}")

    teste += " products_structured: " + str(products_structured) + " - "
    candidates = []
    for product_name, product_data in products_structured.items():
        sku_candidate = str(product_data.get("sku", "") or "")
        if not sku_candidate:
            continue

        try:
            price_candidate = float(product_data.get("price", 0))
        except (TypeError, ValueError):
            raise ValueError(f"Erro ao converter preço para float: {product_data.get('price', 0)}")

        candidates.append(
            {
                "sku_id": sku_candidate,
                "name": product_data.get("name", product_name),
                "price": price_candidate,
                "image_url": product_data.get("image_url"),
                "unit_multiplier": product_data.get("unit_multiplier", 1.0),  # Incluir informações de unidade
                "measurement_unit": product_data.get("measurement_unit", "un"),
            }
        )
    teste += "candidates: " + str(candidates) + " - "
    candidates.sort(key=lambda item: abs(item["price"] - original_price))
    return candidates[:max_products], teste



def cart_simulation(base_url, products_details, seller, quantity, country, engine, teste=None):
    """
    Performs cart simulation to check availability and delivery channel.

    Args:
        base_url (str): Base URL of the API
        products_details (dict|list): Estrutura simples retornada pela busca ou lista normalizada
        seller (str): Seller ID
        quantity (int): Quantity of products
        country (str): Delivery country
        engine: Engine object para logging

    Returns:
        list: List of details of selected products
    """
    normalized_products = []

    if isinstance(products_details, dict):
        for product_name, product_data in products_details.items():
            sku_id = str(product_data.get("sku", "") or "")
            if not sku_id:
                continue

            # Priorizar quantidade já convertida para unidades quando disponível
            if "quantity_in_units_for_cart_sim" in product_data:
                api_quantity = int(round(product_data["quantity_in_units_for_cart_sim"]))
                teste += f"cart_simulation: usando quantity_in_units_for_cart_sim={api_quantity} - "
            elif "quantity_in_units" in product_data:
                # Fallback legado (não deve mais ser usado fora de cart_simulation)
                api_quantity = int(round(product_data["quantity_in_units"]))
                teste += f"cart_simulation: usando quantity_in_units (fallback)={api_quantity} - "
            else:
                # Obter informações de unidade para conversão de quantidade
                unit_multiplier = float(product_data.get("unit_multiplier", 1.0))
                measurement_unit = product_data.get("measurement_unit", "").lower()
                qty_from_product = product_data.get("quantity", quantity)

                # Calcular quantidade para a API
                if measurement_unit == "kg" and unit_multiplier > 0 and unit_multiplier != 1.0:
                    # Converter kg para unidades: quantidade_kg / peso_por_unidade
                    # API do carrinho requer quantidade em unidades como inteiro
                    units_float = qty_from_product / unit_multiplier
                    api_quantity = int(round(units_float))
                    teste += f"cart_simulation: convertendo quantidade {qty_from_product}kg para {api_quantity} unidades (unit_multiplier={unit_multiplier}, calculado={units_float}) - "
                else:
                    # Garantir que quantidade seja inteira para produtos por unidade
                    api_quantity = int(round(qty_from_product)) if isinstance(qty_from_product, float) else int(qty_from_product)

            normalized_products.append(
                {
                    "sku": sku_id,
                    "name": product_data.get("name", product_name),
                    "price": product_data.get("price", 0),
                    "image_url": product_data.get("image_url"),
                    "api_quantity": api_quantity,  # Quantidade ajustada para a API
                }
            )
    else:
        for product in products_details:
            if isinstance(product, dict):
                sku_id = str(product.get("sku") or product.get("sku_id") or "")
                if not sku_id:
                    continue

                # Priorizar quantidade em unidades quando disponível na estrutura
                api_quantity = None
                if "quantity_in_units_for_cart_sim" in product:
                    api_quantity = int(round(product["quantity_in_units_for_cart_sim"]))
                    teste += f"cart_simulation: usando quantity_in_units_for_cart_sim(lista)={api_quantity} - "
                elif "quantity_in_units" in product:
                    api_quantity = int(round(product["quantity_in_units"]))
                    teste += f"cart_simulation: usando quantity_in_units(lista, fallback)={api_quantity} - "
                else:
                    qty_from_product = product.get("quantity", quantity)
                    api_quantity = int(round(qty_from_product)) if isinstance(qty_from_product, float) else int(qty_from_product)

                normalized_products.append(
                    {
                        "sku": sku_id,
                        "name": product.get("name"),
                        "price": product.get("price", product.get("original_price", 0)),
                        "image_url": product.get("image_url"),
                        "api_quantity": api_quantity,
                    }
                )

    if not normalized_products:
        teste += "cart_simulation: normalized_products vazio - "
        return [], teste

    # Usar api_quantity se disponível (já convertida e arredondada para inteiro), senão usar quantity original (também como inteiro)
    items = [
        {
            "id": product["sku"], 
            "quantity": int(product.get("api_quantity", int(round(quantity)))),  # API requer inteiro
            "seller": seller
        } 
        for product in normalized_products
    ]
    teste += f"cart_simulation: items_enviados={items} - "
    url = f"{base_url}/api/checkout/pub/orderForms/simulation"
    payload = {"items": items, "country": country}


    try:
        # Timeout reduzido para evitar timeout do contexto de requisição
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        response_data = response.json()


        selected_products = []
        simulation_items = {
            item.get("id"): item
            for item in response_data.get("items", [])
            if item.get("availability", "").lower() == "available"
        }
        teste += f"cart_simulation: simulation_items_keys={list(simulation_items.keys())} - "


        for product_detail in normalized_products:
            sku_id = product_detail.get("sku")
            if sku_id in simulation_items:
                selected_products.append(product_detail)

        teste += f"cart_simulation: selected_products={selected_products} - "
        return selected_products, teste
    except requests.exceptions.RequestException as e:
        teste += f"cart_simulation: erro={e} - "
        return [], teste


def search_products_by_merchandise_group(merchandise_group, original_price, base_url, engine, original_sku_id=None, original_quantity=None, teste=None):
    """
    Busca produtos similares usando o Merchandise_Group.
    Regras principais de preço:
      - Preço unitário do substituto pode ser no máximo 20% MAIOR que o original.
      - Valor total (preço_unitário_sub * quantidade_sub) NUNCA pode ultrapassar o valor total original.
      - Permite produtos mais baratos; nestes casos a quantidade será ajustada depois para se aproximar do total original.

    Args:
        merchandise_group (str): Merchandise_Group do produto original (ex: "M06020210" ou "P03040104")
        original_price (float): Preço unitário original do produto removido
        base_url (str): URL base da API VTEX
        engine: Engine object para logging
        original_sku_id (str|int): SKU do produto original (para excluir dos resultados)
        original_quantity (int): Quantidade original do produto

    Returns:
        dict: Estrutura de produtos encontrados, organizada por nome do produto,
              contendo apenas as chaves "sku", "name" e "price".
    """
    if teste is None:
        teste = ""

    products_less_or_equal = {}
    products_up_to_twenty = {}
    original_price = round(float(original_price or 0), 3)
    # Manter quantidade como float para suportar produtos vendidos por peso (kg) com quantidades decimais
    # Limitar a 3 casas decimais
    original_quantity = round(float(original_quantity or 1), 3) if original_quantity is not None else 1.0
    
    # Calcular valor total original do pedido
    # REGRA GLOBAL:
    #   - O valor total do substituto NUNCA deve ultrapassar o valor original.
    #   - O preço unitário pode ser até 20% maior, desde que respeite a regra do valor total.
    # Limitar a 3 casas decimais
    original_total_value = round(original_price * original_quantity, 3)
    
    # Adicionar logs de debug
    teste += f"search_by_merch_group: original_sku_id={original_sku_id} original_price={original_price} original_quantity={original_quantity} original_total={original_total_value} - "
    
    try:
        # Montar URL da API de busca
        # fq=specificationFilter_247:{merchandise_group} - filtra por Merchandise_Group
        # O=OrderByPriceASC - ordena por preço ascendente
        # _from=0&_to=49 - retorna até 50 produtos
        search_url = (
            f"{base_url}/api/catalog_system/pub/products/search"
            f"?_from=0&_to=49"
            f"&fq=specificationFilter_247:{merchandise_group}"
            f"&O=OrderByPriceASC"
        )
        
        
        # Timeout reduzido para evitar timeout do contexto de requisição
        response = requests.get(search_url, timeout=15)
        response.raise_for_status()
        products = response.json() 
        
        if not isinstance(products, list):
            teste += f"search_by_merch_group: formato_resposta_invalido={type(products)} - "
            return {}, False, False, teste
        
        teste += f"search_by_merch_group: total_produtos_encontrados={len(products)} - "
        
        produtos_processados = 0
        itens_processados = 0
        produtos_rejeitados_por_20 = 0  # Contador de produtos rejeitados por exceder +20%
        for product in products:
            if not product.get("items"):
                teste += f"search_by_merch_group: pulando_produto_sem_items={product.get('productId')} - "
                continue
            
            product_name_vtex = product.get("productName", "")
            if not product_name_vtex:
                continue

            produtos_processados += 1
            total_items_produto = len(product.get("items", []))
            teste += f"search_by_merch_group: produto_{produtos_processados} items={total_items_produto} - "

            # Considerar TODOS os itens do produto, não apenas o primeiro
            for item in product.get("items", []):
                itens_processados += 1
                if not item:
                    continue
                
                item_id = item.get("itemId")
                if item_id is None:
                    continue
                
                # Excluir o SKU original se for o mesmo
                if str(item_id) == str(original_sku_id):
                    teste += f"search_by_merch_group: SKU_original_excluido={item_id} - "
                    continue
                
                # Obter imagem do item
                images = item.get("images", [])
                image_url = images[0].get("imageUrl") if images else ""
                
                sku_id = str(item_id)
                sku_name = item.get("nameComplete", item.get("name", "")) or product_name_vtex
                
                # Obter informações de unidade do item
                measurement_unit = item.get("measurementUnit", "").lower()
                unit_multiplier = float(item.get("unitMultiplier", 1.0))
                
                price = None
                for seller in item.get("sellers", []):
                    offer = seller.get("commertialOffer", {})
                    price_value = offer.get("Price")
                    full_selling_price = offer.get("FullSellingPrice")  # Preço por kg quando disponível
                    teste += f"search_by_merch_group: price_value_raw={price_value} full_selling_price={full_selling_price} unit_multiplier={unit_multiplier} measurement_unit={measurement_unit} - "
                    if isinstance(price_value, (int, float)):
                        # Preço pode vir em centavos ou reais - normalizar para reais
                        # Na VTEX: se for inteiro >= 100, provavelmente está em centavos
                        # Se for decimal < 100, provavelmente está em reais
                        if isinstance(price_value, int) and price_value >= 100:
                            # Inteiro >= 100: provavelmente está em centavos
                            price_raw = float(price_value) / 100.0
                            teste += f"search_by_merch_group: price_normalizado_de_centavos={price_value}->{price_raw} - "
                        elif price_value > 1000:
                            # Float > 1000: provavelmente está em centavos
                            price_raw = float(price_value) / 100.0
                            teste += f"search_by_merch_group: price_normalizado_de_centavos={price_value}->{price_raw} - "
                        else:
                            # Decimal < 100 ou inteiro < 100: provavelmente já está em reais
                            price_raw = float(price_value)
                        
                        # Para produtos vendidos por peso (kg), normalizar preço para por kg
                        # Se houver unitMultiplier diferente de 1, o preço está por unidade, não por kg
                        if measurement_unit == "kg" and unit_multiplier > 0 and unit_multiplier != 1.0:
                            # Preço está por unidade, converter para por kg
                            # Se FullSellingPrice estiver disponível, usar ele (já está por kg)
                            if full_selling_price and isinstance(full_selling_price, (int, float)):
                                # FullSellingPrice pode estar em centavos se for >= 100
                                if isinstance(full_selling_price, int) and full_selling_price >= 100:
                                    price = float(full_selling_price) / 100.0
                                elif full_selling_price > 1000:
                                    price = float(full_selling_price) / 100.0
                                else:
                                    price = float(full_selling_price)
                                teste += f"search_by_merch_group: usando FullSellingPrice={price} (preço por kg) - "
                            else:
                                # Calcular preço por kg: preço por unidade / peso por unidade
                                price = price_raw / unit_multiplier
                                teste += f"search_by_merch_group: preço_normalizado_por_kg={price} (price_raw={price_raw} / unit_multiplier={unit_multiplier}) - "
                        else:
                            # Produto vendido por unidade ou unitMultiplier = 1, usar preço direto
                            price = price_raw
                            teste += f"search_by_merch_group: preço_por_unidade={price} - "
                        break

                if price is None:
                    teste += f"search_by_merch_group: SKU {sku_id} sem preço - "
                    continue

                # Usar SKU como chave única para permitir múltiplas variações do mesmo produto
                unique_key = f"{product_name_vtex}_{sku_id}"
                
                variation = {
                    "sku": sku_id,
                    "name": product_name_vtex or sku_name,
                    "price": price,
                    "image_url": image_url,
                    "unit_multiplier": unit_multiplier,  # Para conversão de quantidade
                    "measurement_unit": measurement_unit  # Para identificar produtos por peso
                }
                
                # REGRA: O valor total NUNCA deve ultrapassar o original.
                # O preço unitário já foi validado para não ultrapassar +20% (mais à frente),
                # e a quantidade será calculada com base no valor total original.
                
                # Calcular quantidade máxima possível baseada no valor total ORIGINAL (não +20%)
                # Quantidade máxima = valor_total_original / preço_unitário_novo
                # Manter como float para suportar produtos vendidos por peso (kg) com quantidades decimais
                # Limitar a 3 casas decimais
                if price > 0:
                    max_quantity_possible = round(original_total_value / price, 3)
                else:
                    max_quantity_possible = 0.0
                
                # Calcular valor total com a quantidade máxima possível
                total_value_with_max_qty = round(price * max_quantity_possible, 3)
                
                # Adicionar log detalhado para cada produto avaliado
                teste += f"search_by_merch_group: avaliando SKU={sku_id} price={price} max_qty_possible={max_quantity_possible} total_com_max_qty={total_value_with_max_qty} vs orig_total={original_total_value} - "

                # Se não conseguir comprar pelo menos uma quantidade mínima (0.001 para produtos por peso, 1 para produtos por unidade)
                # Usar 0.001 como mínimo para aceitar quantidades decimais de produtos vendidos por peso
                min_quantity_threshold = 0.001 if original_quantity < 1 else 1.0
                if max_quantity_possible < min_quantity_threshold:
                    teste += f"search_by_merch_group: SKU={sku_id} REJEITADO (max_qty={max_quantity_possible} < {min_quantity_threshold}) - "
                    continue

                # Calcular aumento percentual do preço unitário (regra principal: até +20%)
                price_increase_percent = ((price - original_price) / original_price) * 100 if original_price > 0 else 0
                if price_increase_percent > 20.0:
                    produtos_rejeitados_por_20 += 1
                    teste += f"search_by_merch_group: SKU={sku_id} REJEITADO (price_increase={price_increase_percent:.1f}% > 20%) - "
                    continue
                
                # REGRA: O valor total NUNCA deve ultrapassar o original
                if total_value_with_max_qty <= original_total_value:
                    # Produto com valor total menor ou igual ao original - ACEITAR
                    if unique_key not in products_less_or_equal:
                        products_less_or_equal[unique_key] = variation
                        teste += f"search_by_merch_group: <=orig_ADICIONADO SKU={sku_id} price={price} max_qty={max_quantity_possible} total={total_value_with_max_qty} price_increase={price_increase_percent:.1f}% - "
                    continue

                # Se chegou aqui, preço dentro de +20% mas total acima do original -> guardar no bucket de +20
                if unique_key not in products_up_to_twenty:
                    products_up_to_twenty[unique_key] = variation
                    teste += f"search_by_merch_group: <=+20_ADICIONADO SKU={sku_id} price={price} max_qty={max_quantity_possible} total={total_value_with_max_qty} price_increase={price_increase_percent:.1f}% - "
        
        teste += f"search_by_merch_group: resumo produtos_processados={produtos_processados} itens_processados={itens_processados} produtos_rejeitados_por_20={produtos_rejeitados_por_20} - "
        
        # Verificar se encontrou produtos mas todos foram rejeitados por exceder +20%
        found_but_rejected_by_20 = produtos_rejeitados_por_20 > 0 and len(products_less_or_equal) == 0 and len(products_up_to_twenty) == 0
        
        if products_less_or_equal:
            # products_less_or_equal: produtos em que é possível montar um cenário com
            # valor_total_substituto <= valor_total_original
            teste += f"search_by_merch_group: <=orig_total={len(products_less_or_equal)} - "
            # has_products_less_or_equal = False indica que temos opção "segura" para auto-substituição
            return products_less_or_equal, False, found_but_rejected_by_20, teste

        # products_up_to_twenty: produtos com preço unitário dentro de +20%,
        # mas que exigiriam um cenário em que o valor total ultrapassa o original
        # (na prática, hoje a regra de quantidade impede que esses sejam usados,
        # mas mantemos o bucket por compatibilidade).
        teste += f"search_by_merch_group: <=+20_total={len(products_up_to_twenty)} - "
        # has_products_less_or_equal = True indica que só temos opções "limite +20"
        return products_up_to_twenty, True, found_but_rejected_by_20, teste
        
    except requests.exceptions.Timeout:
        teste += f"search_by_merch_group: timeout - "
        return {}, False, False, teste
        
    except requests.exceptions.RequestException as e:
        teste += f"search_by_merch_group: request_error={str(e)[:100]} - "
        if hasattr(e, "response") and e.response is not None:
            teste += f"search_by_merch_group: status={e.response.status_code} - "
        return {}, False, False, teste
        
    except (KeyError, IndexError, TypeError) as e:
        teste += f"search_by_merch_group: parse_error={e} - "
        return {}, False, False, teste

    except Exception as e:
        teste += f"search_by_merch_group: unexpected_error={e} - "
        return {}, False, False, teste


def get_vtex_order_details(order_id, engine):
    """
    Requisita detalhes do pedido na API da VTEX
    """
    try:
        base_url = "https://hmlzaffari.myvtex.com"
        url = f"{base_url}/api/oms/pvt/orders/{order_id}"

        headers = {
            "X-Vtex-Api-Apptoken": "YTIWVKUCXEGUUQOETOZXZYTLJHHGNFYUOMCJJBBNHJLEBKESLTHKWRVPNZPKYOYDYSMTKQQSFWDUECGNRGEUSHZJUMXKCQKMCZJPXUXQHWHLNZDFUQCPGXTOJXLBLPWV",
            "X-Vtex-Api-Appkey": "vtexappkey-hmlzaffari-KYTJNJ",
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException:
        return None


def get_sku_data_from_vtex(sku_id, engine):
    """
    Consulta o SKU na API da VTEX para obter dados completos do produto.
    
    Args:
        sku_id (str|int): ID do SKU a ser consultado
        engine: Engine object para logging
    
    Returns:
        dict|None: Dados do SKU com keys: merchandise_group, unit_multiplier, measurement_unit
                   ou None se houver erro
    """
    try:
        base_url = "https://hmlzaffari.myvtex.com"
        url = f"{base_url}/api/catalog_system/pvt/sku/stockkeepingunitbyid/{sku_id}"
        
        headers = {
            "X-Vtex-Api-Apptoken": "YTIWVKUCXEGUUQOETOZXZYTLJHHGNFYUOMCJJBBNHJLEBKESLTHKWRVPNZPKYOYDYSMTKQQSFWDUECGNRGEUSHZJUMXKCQKMCZJPXUXQHWHLNZDFUQCPGXTOJXLBLPWV",
            "X-Vtex-Api-Appkey": "vtexappkey-hmlzaffari-KYTJNJ",
        }
        
        # Timeout reduzido para evitar timeout do contexto de requisição
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        sku_data = response.json()
        
        # Buscar Merchandise_Group nas ProductSpecifications
        merchandise_group = None
        product_specifications = sku_data.get("ProductSpecifications", [])
        for spec in product_specifications:
            field_id = spec.get("FieldId")
            field_name = spec.get("FieldName")
            if field_id == 247 or field_name == "Merchandise_Group":
                field_values = spec.get("FieldValues", [])
                if field_values and len(field_values) > 0:
                    merchandise_group = field_values[0]
                    break
        
        # Obter UnitMultiplier e MeasurementUnit diretamente do JSON
        unit_multiplier = float(sku_data.get("UnitMultiplier", 1.0))
        measurement_unit = sku_data.get("MeasurementUnit", "un").lower()
        
        return {
            "merchandise_group": merchandise_group,
            "unit_multiplier": unit_multiplier,
            "measurement_unit": measurement_unit
        }
        
    except requests.exceptions.Timeout:
        engine.log.debug(f"⏰ Timeout ao consultar SKU {sku_id}")
        return None
        
    except requests.exceptions.RequestException as e:
        engine.log.debug(f"❌ Erro ao consultar SKU {sku_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            engine.log.debug(f"   Status HTTP: {e.response.status_code}")
            engine.log.debug(f"   Resposta: {e.response.text[:200]}")
        return None
        
    except (KeyError, IndexError, TypeError, ValueError) as e:
        engine.log.debug(f"❌ Erro ao processar resposta do SKU {sku_id}: {e}")
        return None
        
    except Exception as e:
        engine.log.debug(f"❌ Erro inesperado ao obter dados do SKU {sku_id}: {e}")
        return None


def get_sku_merchandise_group(sku_id, engine):
    """
    Consulta o SKU na API da VTEX para obter o Merchandise_Group.
    (Função mantida para compatibilidade, agora usa get_sku_data_from_vtex)
    
    Args:
        sku_id (str|int): ID do SKU a ser consultado
        engine: Engine object para logging
    
    Returns:
        str|None: Valor do Merchandise_Group (ex: "M06020210") ou None se não encontrar
    """
    sku_data = get_sku_data_from_vtex(sku_id, engine)
    if sku_data:
        return sku_data.get("merchandise_group")
    return None


def format_phone_to_urn(phone):
    """
    Formata o telefone para o padrão URN (whatsapp:5511999999999 ou whatsapp:555133334444)
    Garante que o número está no padrão brasileiro, adicionando o 9 para celulares se necessário.
    """
    phone_clean = re.sub(r"[^\d]", "", phone)
    if phone_clean.startswith("00"):
        phone_clean = phone_clean[2:]
    if phone_clean.startswith("55"):
        phone_clean = phone_clean[2:]
    # Agora phone_clean deve ser DDD + número
    if len(phone_clean) == 10:
        # Se for celular (começa com 9 após DDD), mantém. Se não, adiciona 9 para celulares.
        ddd = phone_clean[:2]
        numero = phone_clean[2:]
        if not numero.startswith("9"):
            # Adiciona o 9 para celulares (padrão Brasil)
            numero = "9" + numero
        phone_clean = ddd + numero
    # Se já tem 11 dígitos, mantém
    # Se for fixo (8 dígitos após DDD), não adiciona o 9
    return f"whatsapp:55{phone_clean}"


def get_weni_contact(urn, engine):
    """
    Consulta contato na API da Weni
    """
    try:
        url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
        headers = {"Authorization": "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return results[0] if results else None

    except requests.exceptions.RequestException as e:
        return None


def update_weni_contact(urn, name, skus_list, engine, order_id=None, teste=""):
    """
    Atualiza contato na API da Weni com nova lista de SKUs e order_id atual
    
    Args:
        urn (str): URN do contato
        name (str): Nome do contato
        skus_list (list): Lista de SKUs processadas
        engine: Engine object para logging
        order_id (str, optional): ID do pedido atual para armazenar
        teste (str, optional): String de debug para acumular logs
    Returns:
        tuple: (success: bool, teste: str)
    """
    debug_info = f"update_weni_contact: INICIO urn={urn} name={name} skus_list={skus_list} order_id={order_id} - "
    try:
        url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
        headers = {
            "Authorization": "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70",  #token do projeto
            "Content-Type": "application/json",
        }

        fields = {"sku": json.dumps(skus_list)}
        """ if order_id:
            fields["order_id"] = order_id """

        payload = {"name": name, "fields": fields}
        debug_info += f"update_weni_contact: PREPARANDO_REQUISICAO url={url} payload={json.dumps(payload)} - "
        
        response = requests.post(url, json=payload, headers=headers)
        debug_info += f"update_weni_contact: RESPOSTA_RECEBIDA status_code={response.status_code} - "
        
        try:
            response.raise_for_status()
            debug_info += f"update_weni_contact: raise_for_status_OK - "
        except requests.exceptions.HTTPError as http_err:
            debug_info += f"update_weni_contact: HTTP_ERROR={str(http_err)} response_text={response.text[:500]} - "
            return False, teste + debug_info
        except Exception as raise_err:
            debug_info += f"update_weni_contact: ERRO_RAISE_FOR_STATUS={str(raise_err)} response_text={response.text[:500] if hasattr(response, 'text') else 'N/A'} - "
            return False, teste + debug_info
        
        if response.status_code == 200:
            debug_info += f"update_weni_contact: SUCESSO status_code=200 response_text={response.text[:200]} - "
            return True, teste + debug_info
        else:
            debug_info += f"update_weni_contact: FALHA status_code={response.status_code} response_text={response.text[:500]} - "
            return False, teste + debug_info
    except requests.exceptions.RequestException as e:
        debug_info += f"update_weni_contact: REQUEST_EXCEPTION={str(e)} tipo={type(e).__name__} - "
        return False, teste + debug_info
    except Exception as e:
        debug_info += f"update_weni_contact: EXCEPTION_GERAL={str(e)} tipo={type(e).__name__} - "
        return False, teste + debug_info


def save_produtos_escolhidos_to_weni(urn, produtos_escolhidos, engine):
    """
    Salva a lista de produtos_escolhidos nas variáveis de contato da Weni,
    usando o padrão items_partN (aqui, apenas items_part1).

    Isso garante que, antes de qualquer disparo de fluxo Weni/WhatsApp,
    os dados estejam persistidos para uso posterior (ex.: CodeAction com disparar_flow=true).
    """
    if not urn or not produtos_escolhidos:
        return False, ""

    try:
        url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
        headers = {
            "Authorization": "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70",  # mesmo token do projeto
            "Content-Type": "application/json",
        }

        # Garantir que cada item tenha as chaves esperadas
        # A quantidade é sempre salva em kg (ponto flutuante); conversions para unidades
        # são feitas apenas quando necessário (ex.: cart_simulation)
        items_normalizados = []
        for p in produtos_escolhidos:
            if not isinstance(p, dict):
                continue
            sku = p.get("sku") or p.get("id")
            if not sku:
                continue
            items_normalizados.append(
                {
                    "sku": str(sku),
                    "name": p.get("name", ""),
                    "price": p.get("price", 0),
                    "quantity": p.get("quantity", 1),  # Quantidade em kg (float)
                    # quantity_in_units não é mais persistido na Weni
                    "unit_multiplier": p.get("unit_multiplier", 1.0),
                    "measurement_unit": p.get("measurement_unit", "un"),
                    "image_url": p.get("image_url", ""),
                }
            )

        if not items_normalizados:
            return False, ""

        # Usar apenas items_part1; se no futuro houver mais itens, podemos paginar
        fields = {
            "items_part1": json.dumps(
                {"items": items_normalizados, "chunk_index": 1, "chunk_size": len(items_normalizados)},
                ensure_ascii=False,
            )
        }

        payload = {"fields": fields}
        # Timeout reduzido para evitar timeout do contexto de requisição
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        response.raise_for_status()

        success = 200 <= response.status_code < 300
        teste_local = (
            f"save_produtos_escolhidos_to_weni: status={response.status_code} "
            f"success={success} itens={len(items_normalizados)} - "
        )
        return success, teste_local
    except requests.exceptions.RequestException as e:
        teste_local = f"save_produtos_escolhidos_to_weni: request_error={e} - "
        return False, teste_local
    except Exception as e:
        teste_local = f"save_produtos_escolhidos_to_weni: unexpected_error={e} - "
        return False, teste_local


def get_processed_skus_from_weni(contact_data, engine=None):
    """
    Extrai lista de SKUs já processadas do contato da Weni
    """
    try:
        if contact_data and "fields" in contact_data:
            # Tentar primeiro com 'sku' minúsculo, depois 'SKU' maiúsculo
            sku_field = contact_data["fields"].get("sku", contact_data["fields"].get("SKU", "[]"))
            if sku_field is None:
                sku_field = "[]"
            return json.loads(sku_field)
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        if engine:
            pass
    return []


def get_order_id_from_weni(contact_data, engine=None):
    """
    Extrai o último order_id processado do contato da Weni
    """
    try:
        if contact_data and "fields" in contact_data:
            # Buscar campo 'order_id' ou 'order_id'
            order_id_field = contact_data["fields"].get("order_id", contact_data["fields"].get("order_id", ""))
            return order_id_field if order_id_field else None
    except (KeyError, TypeError) as e:
        if engine:
            pass
    return None


def get_weni_contact_robust(phone, engine):
    """
    Busca contato na Weni tentando com e sem o dígito 9 após o DDD.
    Retorna (contato, urn_utilizada)
    """
    urn_with_9 = format_phone_to_urn(phone)
    urn_without_9 = None
    phone_clean = re.sub(r"[^\d]", "", phone)
    if phone_clean.startswith("00"):
        phone_clean = phone_clean[2:]
    if phone_clean.startswith("55"):
        phone_clean = phone_clean[2:]
    if len(phone_clean) == 11 and phone_clean[2] == "9":
        # Remove o 9 após o DDD
        phone_wo_9 = phone_clean[:2] + phone_clean[3:]
        urn_without_9 = f"whatsapp:55{phone_wo_9}"
    # 1. Tenta buscar com o 9
    contact = get_weni_contact(urn_with_9, engine)
    if contact:
        return contact, urn_with_9
    # 2. Tenta buscar sem o 9
    if urn_without_9:
        contact = get_weni_contact(urn_without_9, engine)
        if contact:
            return contact, urn_without_9
    return None, urn_with_9


def start_weni_flow(phone, produtos_escolhidos, produto_antigo, order_id, commerce_id, name, engine, first_contact, is_promotional=False, is_journey=False, teste=None):
    """
    Dispara fluxo na Weni com os produtos escolhidos para substituição.
    
    Args:
        phone (str): Telefone do cliente
        produtos_escolhidos (list): Lista de produtos escolhidos para substituição
        produto_antigo (dict): Dados do produto original removido
        order_id (str): ID do pedido
        commerce_id (str): ID do comércio
        name (str): Nome do cliente
        engine: Engine object para logging
        first_contact: Se é o primeiro contato
        is_promotional (bool): Se é item promocional
        is_journey (bool): Se é um fluxo de journey
        teste (str): String para acumular logs de debug
    
    Returns:
        tuple: (bool, str) - (success, teste_atualizado)
    """
    if teste is None:
        teste = ""
    
    teste += f"start_weni_flow: inicio phone={bool(phone)} produtos_escolhidos_count={len(produtos_escolhidos) if produtos_escolhidos else 0} order_id={order_id} is_promotional={is_promotional} is_journey={is_journey} - "
    
    try:
        if not phone:
            teste += f"start_weni_flow: ERRO phone_vazio - "
            return False, teste
        
        # Formatar telefone para URN (remover o "whatsapp:" do início se existir)
        urn = phone.replace("+", "whatsapp:") if not phone.startswith("whatsapp:") else phone
        teste += f"start_weni_flow: urn_formatado={urn} - "
        
        url = "https://flows.weni.ai/api/v2/flow_starts.json"
        headers = {
            "Authorization": f"Token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70",
            "Content-Type": "application/json"
        }
        body = {
            "flow": "c6924c40-c6d9-40c9-b0f4-671f5d38e597",
            "urns": [urn],
            "params": {
                "customer_phone": phone,
                "name": name,
                "order_id": order_id,
                "produtos_escolhidos": json.dumps(produtos_escolhidos),
                "nome_produto_antigo": produto_antigo.get("name", ""),
                "produto_antigo": json.dumps(produto_antigo),
                "commerce_id": commerce_id,
                "sku_original": str(produto_antigo.get("sku", "")),
                "first_contact": first_contact,
                "is_promotional": is_promotional,
                "is_journey": is_journey
            }
        }
        
        teste += f"start_weni_flow: preparando_requisicao url={url} - "
        
        # Timeout reduzido para evitar timeout do contexto de requisição
        response = requests.post(url, headers=headers, json=body, timeout=15)
        status_code = response.status_code
        teste += f"start_weni_flow: status_code={status_code} - "
        
        response.raise_for_status()
        
        teste += f"start_weni_flow: SUCESSO - "
        return True, teste
        
    except requests.exceptions.RequestException as e:
        teste += f"start_weni_flow: request_error={str(e)} - "
        if hasattr(e, "response") and e.response is not None:
            try:
                teste += f"start_weni_flow: response_status={e.response.status_code} response_text={e.response.text[:200]} - "
            except Exception:
                pass
        return False, teste
    except Exception as e:
        teste += f"start_weni_flow: unexpected_error={type(e).__name__}:{str(e)} - "
        return False, teste


def convert_image_to_base64(url: str):
    """Converte uma imagem remota para base64, similar à implementação de recebedor.py."""
    if not url or not isinstance(url, str) or not url.strip():
        return ""

    try:
        response = httpx.get(url, follow_redirects=True)
        response.raise_for_status()
        image_data = response.content
        base64_encoded = base64.b64encode(image_data).decode("utf-8")
        return base64_encoded
    except httpx.HTTPStatusError:
        return ""
    except httpx.RequestError:
        return ""
    except Exception:
        return ""


def get_base64(link, engine=None):
    """Wrapper simples para manter assinatura compatível com chamadas existentes."""
    return convert_image_to_base64(link)


def send_whatsapp_flow_after_weni(phone, produtos_escolhidos, produto_antigo, engine, flow_id="1584991322669299"):
    """
    Dispara um WhatsApp Flow via Facebook Graph API com os itens sugeridos e opção de não substituir.

    Requisitos (engine.params):
      - numberidMeta: ID do número do WhatsApp Business
      - meta_token: Token Bearer do Graph API
      - tempo (opcional): minutos a exibir no rodapé
    """
    try:
        
        numberidMeta = "834197016447639"

        meta_token = "EAAFYX7IlaB8BOZBayFJsnOft4UHPXRbevMGOsu0xHpjYMS2oQOHwza2r8IPnt98u2JGCClbRrI4jdASkzFP2ZCxPPGskGmInKT0W0r0ZC6DYQlXnPZAZA6iSBCf0C9oeYLKahi4XqLYSEgZATzFfv5RS6vz25UxgZCLcMhMKISCOnNd7W3URBlkOczZBhpQDc1p6"
    

        if not numberidMeta or not meta_token:
            return False

        original_name = str(produto_antigo.get("name", ""))
        original_price = float(produto_antigo.get("price", 0))
        original_qty = produto_antigo.get("quantity", 1)
        original_unit = produto_antigo.get("unit", "un")  # "kg" ou "un"
        original_unit_multiplier = float(produto_antigo.get("unit_multiplier", 1.0))
        original_image_url = produto_antigo.get("photo_url", "")
        
        # Validar se quantidade existe e não é zero
        if original_qty is None or original_qty == 0:
            # Tentar obter quantidade de outra fonte ou usar valor padrão
            original_qty = produto_antigo.get("quantity", 1)
            if original_qty is None or original_qty == 0:
                original_qty = 1  # Valor padrão
        
        # Formatar produto original
        if original_unit == "kg":
            # Converter quantidade em kg para número aproximado de unidades (para exibição), usando floor
            qty_kg = float(original_qty or 0)
            units = 0
            if original_unit_multiplier and original_unit_multiplier > 0:
                units_float = qty_kg / float(original_unit_multiplier)
                units = int(units_float) if units_float > 0 else 0
            if units < 1:
                units = 1
            peso_aprox = round(qty_kg, 3)
            # Exibir quantidade em unidades (floor), peso aproximado e preço por kg
            prod_text = f"{original_name} \nQde {units} aprox. {peso_aprox}kg - R$ {original_price}/kg"
        else:
            # Produto por unidade
            original_qty_display = int(original_qty) if isinstance(original_qty, float) else original_qty
            prod_text = f"{original_name} \nQde {original_qty_display}\nR$ {original_price}"

        subs_list = []
        for item in produtos_escolhidos[:5]:
            item_unit = item.get("measurement_unit", "un")

            if item_unit == "kg":
                # Produtos vendidos por kg:
                # - quantity já está em kg (float)
                # - calcular unidades aproximadas usando unit_multiplier
                quantity_kg = float(item.get("quantity", 0) or 0)
                unit_mult = float(item.get("unit_multiplier", 1.0) or 1.0)
                preco_kg = float(item.get("price", 0) or 0)

                units_float = quantity_kg / unit_mult if unit_mult > 0 else 0
                quantity_units = int(units_float) if units_float > 0 else 0
                if quantity_units < 1:
                    quantity_units = 1

                peso_aprox = round(quantity_kg, 3)
                valor_total = round(preco_kg * peso_aprox, 2)
                metadata = f"Qde {quantity_units} aprox. {peso_aprox}kg • Total aprox. R$ {valor_total}"
            else:
                # Produtos por unidade:
                # - se unit_multiplier == 1, quantity já está em unidades
                # - caso contrário, converter quantity (kg) para unidades
                quantity_base = float(item.get("quantity", 1) or 1)
                unit_mult = float(item.get("unit_multiplier", 1.0) or 1.0)

                if unit_mult == 1.0:
                    quantity_units = int(quantity_base) if quantity_base >= 1 else 1
                else:
                    quantity_units = int(quantity_base / unit_mult) if unit_mult > 0 else int(quantity_base)
                    if quantity_units < 1:
                        quantity_units = 1

                metadata = f"Qde {quantity_units}"
            
            subs_list.append({
                "id": str(item.get("sku", "")),
                "title": str(item.get("name", "")),
                "description": f"R$ {float(item.get('price', 0))}",
                "metadata": metadata
            })
        subs_list.append({
            "id": "no-sub-1",
            "title": "Remover o item que não foi encontrado",
            "description": "-",
            "metadata": "-",
        })

        # Converter imagem do produto original para base64
        image_base64 = ""
        if original_image_url:
            image_base64 = get_base64(original_image_url, engine=engine)
            engine.log.debug(f"send_whatsapp_flow_after_weni: imagem do produto original convertida para base64 - tamanho={len(image_base64)}")


        flow_data = {
            "src1": image_base64,
            "prod_sub1": prod_text,
            "subs1_products": subs_list,
        }

        urn = f"whatsapp:{phone}"
        graph_url = f"https://graph.facebook.com/v19.0/{numberidMeta}/messages"
        headers = {"Authorization": f"Bearer {meta_token}", "Content-Type": "application/json"}
        body = {
            "messaging_product": "whatsapp",
            "to": urn,
            "type": "interactive",
            "interactive": {
                "type": "flow",
                "header": {"type": "text", "text": "Substituição de Produtos"},
                "body": {"text": "Durante o processo de separação de suas compras, não encontramos um item que você pediu. Mas temos uma ótima notícia ✅! Nossa equipe identificou a possibilidade de substituição por um item similar."},
                "footer": {"text": f"Importante: tempo de espera para resposta é de ⏱️ 5 minutos."},
                "action": {
                    "name": "flow",
                    "parameters": {
                        "mode": "published",
                        "flow_message_version": "3",
                        "flow_id": flow_id,
                        "flow_cta": "Substituir",
                        "flow_action": "navigate",
                        "flow_action_payload": {"screen": "SUBSONE", "data": flow_data},
                    },
                },
            },
        }

        # Timeout reduzido para evitar timeout do contexto de requisição
        resp = requests.post(graph_url, headers=headers, json=body, timeout=15)
        return 200 <= resp.status_code < 300
    except Exception as e:
        engine.log.debug(f"send_whatsapp_flow_after_weni: erro ao enviar o fluxo - {e}")
        return False


def send_replacement_suggestion_to_zaffari(order_id, product_id_original, product_id_replacement, quantity, engine=None):
    """
    Envia sugestão de substituição escolhida para a API da Zaffari (ambiente QA).

    POST https://hml-api.zaffari.com.br/ecommerce/integration-matrix/api/v1/flow/orderProductReplacementSugestion
    Body: {
      "orderId": "string",
      "productIdOriginal": "string",
      "productIdReplacement": "string",
      "quantity": 1
    }
    """
    try:
        base_url = "https://hml-api.zaffari.com.br/ecommerce/integration-matrix"
        url = f"{base_url}/api/v1/flow/orderProductReplacementSugestion"
        payload = {
            "orderId": str(order_id),
            "productIdOriginal": str(product_id_original),
            "productIdReplacement": str(product_id_replacement),
            "quantity": float(quantity),
        }
        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": "df1cbce4af78460895d0299a209d0d5c"
        }
        # Timeout reduzido para evitar timeout do contexto de requisição
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        return 200 <= resp.status_code < 300
    except requests.exceptions.RequestException as e:
        engine.log.debug(f"send_replacement_suggestion_to_zaffari: erro ao enviar a sugestão de substituição - {e}")
        return False
    except Exception as e:
        engine.log.debug(f"send_replacement_suggestion_to_zaffari: erro ao enviar a sugestão de substituição - {e}")
        return False


def send_instaleap_external_data(order_id, product_id, ruptura_message, teste=None):
    """
    Informa ruptura do pedido na API da Zaffari (ambiente QA).

    POST https://hml-api.zaffari.com.br/ecommerce/integration-matrix/api/v1/flow/instaleapExternalData
    Body:
    {
      "orderId": "string",
      "productId": "string",
      "messages": {
        "ruptura": "string"
      }
    }
    
    Args:
        order_id (str): ID do pedido
        product_id (str): ID do produto
        ruptura_message (str): Mensagem de ruptura
        teste (str, optional): String para acumular logs de debug
    
    Returns:
        tuple: (bool, str) - (success, teste_atualizado)
    """
    if teste is None:
        teste = ""
    
    # Validar que product_id não está vazio
    if not product_id or str(product_id).strip() == "":
        teste += f"instaleap: ERRO product_id está vazio (order_id={order_id}) - "
        return False, teste
    
    # Garantir que product_id seja string
    product_id = str(product_id).strip()
    order_id = str(order_id).strip() if order_id else ""
    teste += f"instaleap: order_id={order_id} - product_id={product_id} - ruptura_message={ruptura_message} - "
    
    try:
        base_url = "https://hml-api.zaffari.com.br/ecommerce/integration-matrix"
        url = f"{base_url}/api/v1/flow/instaleapExternalData"
        payload = {
            "orderId": order_id,
            "productId": product_id,
            "messages": {"ruptura": str(ruptura_message or "")},
        }
        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": "df1cbce4af78460895d0299a209d0d5c"
        }
        
        # Logs de debug antes de enviar
        teste += f"instaleap: preparando_requisicao url={url} - "
        teste += f"instaleap: payload={json.dumps(payload)[:200]} - "
        
        # Timeout reduzido para evitar timeout do contexto de requisição
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        
        status_code = resp.status_code
        teste += f"instaleap: status_code={status_code} - "
        
        # Tentar ler a resposta
        try:
            response_text = resp.text[:500] if resp.text else ""
            teste += f"instaleap: response_text={response_text} - "
            # Tentar parsear como JSON se possível
            try:
                response_json = resp.json()
                teste += f"instaleap: response_json={json.dumps(response_json)[:300]} - "
            except Exception:
                pass
        except Exception as e:
            teste += f"instaleap: erro_ao_ler_response={str(e)} - "
        
        success = 200 <= status_code < 300
        teste += f"instaleap: success={success} - "
        
        if not success:
            teste += f"instaleap: FALHOU status={status_code} - "
        
        return success, teste
        
    except requests.exceptions.Timeout as e:
        teste += f"instaleap: timeout_error={str(e)} - "
        return False, teste
    except requests.exceptions.RequestException as e:
        teste += f"instaleap: request_error={str(e)} - "
        if hasattr(e, "response") and e.response is not None:
            try:
                teste += f"instaleap: response_status={e.response.status_code} response_text={e.response.text[:200]} - "
            except Exception:
                pass
        return False, teste
    except Exception as e:
        teste += f"instaleap: unexpected_error={type(e).__name__}:{str(e)} - "
        return False, teste

def process_product_replacement(input_data, vtex_order, replacement_method, engine, client_reference=None, promotional_items=[], teste=None):
    """
    Função unificada para processar replacementBySimilar e contactConfirm
    """

    talk_to_client = True
    # Extrair dados do cliente do webhook (recipient) - SEMPRE do job
    recipient = input_data.get("job", {}).get("recipient", {})
    phone = recipient.get("phone_number", "")
    full_name = recipient.get("name", "")
    if not phone:
        return {
            "type": replacement_method,
            "error": "Telefone não encontrado no pedido",
            "remove": False,
        }
    # Buscar contato na Weni de forma robusta
    contact_data, urn = get_weni_contact_robust(phone, engine)


    
    # Verificar se é um pedido novo (comparar order_id atual com o último armazenado)
    order_id = get_order_id_from_weni(contact_data, engine) if contact_data else None
    current_order_id = client_reference or ""
    processed_skus = []
    if contact_data:
        # Se é um pedido novo (order_id diferente), limpar SKUs processadas
        if order_id and order_id != current_order_id:
            processed_skus = []  # Limpar lista de SKUs para novo pedido
            # Atualizar order_id no contato
            _, teste = update_weni_contact(urn, full_name, [], engine, order_id=current_order_id, teste=teste if teste else "")
        else:
            # Mesmo pedido ou primeiro pedido, usar SKUs processadas existentes
            processed_skus = get_processed_skus_from_weni(contact_data, engine)
            if not order_id and current_order_id:
                # Primeira vez, atualizar order_id
                _, teste = update_weni_contact(urn, full_name, processed_skus, engine, order_id=current_order_id, teste=teste if teste else "")



    # Obter itens REMOVED
    job_items = input_data.get("job", {}).get("job_items", [])
    removed_items = [item for item in job_items if item.get("status") == "REMOVED"]


    # Verificar SKUs para processar
    skus_to_process = [item for item in removed_items if item.get("id") and item.get("id") not in processed_skus]
    if not skus_to_process:
        removed_skus = [item.get("id", "") for item in removed_items if item.get("id")]
        return {
            "type": replacement_method,
            "error": f"nenhuma SKU para verificar, SKUs já verificadas: {removed_skus}",
            "remove": True,
        }


    # Processar primeiro SKU
    item_to_process = skus_to_process[0]
    sku_id = item_to_process["id"]
    teste += f"sku_original_em_avaliacao={sku_id} - "

    # Extrair dados básicos do item (necessário tanto para promocional quanto para normal)
    product_name = item_to_process["name"]
    quantity = item_to_process["quantity"]
    original_price_raw = item_to_process["price"]
    original_image_url = item_to_process.get("photo_url", "")
    original_unit = item_to_process.get("unit", "un")  # "kg" ou "un"
    
    # Buscar UnitMultiplier e MeasurementUnit do produto original via API da VTEX
    sku_data = get_sku_data_from_vtex(sku_id, engine)
    if sku_data:
        original_unit_multiplier = sku_data.get("unit_multiplier", 1.0)
        original_measurement_unit = sku_data.get("measurement_unit", original_unit)
        teste += f"process_product_replacement: sku_data_api unit_multiplier={original_unit_multiplier} measurement_unit={original_measurement_unit} - "
    else:
        # Fallback: tentar obter dos atributos do item
        original_unit_multiplier = float(item_to_process.get("attributes", {}).get("unit_multiplier", 1.0))
        original_measurement_unit = original_unit
        teste += f"process_product_replacement: usando_fallback unit_multiplier={original_unit_multiplier} - "
    
    # Normalizar preço original (pode vir em centavos ou reais)
    if isinstance(original_price_raw, (int, float)):
        # Se o preço for maior que 1000, provavelmente está em centavos
        if original_price_raw > 1000:
            original_price = float(original_price_raw) / 100.0
            teste += f"process_product_replacement: original_price_normalizado_de_centavos={original_price_raw}->{original_price} - "
        else:
            original_price = float(original_price_raw)
            teste += f"process_product_replacement: original_price={original_price} - "
    else:
        original_price = float(original_price_raw or 0)
        teste += f"process_product_replacement: original_price_convertido={original_price} - "

    # Verificar se é item promocional
    if sku_id in promotional_items:
        # Criar produto_antigo completo (com todos os campos necessários para o fluxo Weni)
        produto_antigo_promocional = {
            "sku": str(sku_id),
            "name": product_name,
            "price": original_price,
            "quantity": quantity,
            "photo_url": original_image_url,
            "unit": original_measurement_unit,  # Para formatação no WhatsApp Flow
            "unit_multiplier": original_unit_multiplier,  # Para conversões
        }
        
        # IMPORTANTE: NÃO marcar SKU como processado aqui quando é promocional
        # O SKU só deve ser marcado como processado quando realmente for processado:
        # - Se for para fila: será marcado quando for processado da fila
        # - Se for processado imediatamente: será marcado no bloco de processamento imediato
        # Isso evita que o SKU seja marcado como processado antes de ir para a fila,
        # o que causaria erro quando tentar processar da fila
        teste += f"process_product_replacement: sku_promocional_detectado_nao_marcando_como_processado_ainda={sku_id} - "
        
        return {
            "type": replacement_method,
            "error": "SUBST NAO ADIC - PROMOCIONAL",
            "remove": True,
            "produto_antigo": produto_antigo_promocional,
            "teste": teste,
        }
    
    produto_antigo = {
        "sku": sku_id,
        "name": product_name,
        "price": original_price,
        "quantity": quantity,
        "photo_url": original_image_url,
        "unit": original_measurement_unit,  # Para formatação no WhatsApp Flow
        "unit_multiplier": original_unit_multiplier,  # Para conversões
    }
    
    # Buscar Merchandise_Group do SKU removido (reutilizar dados já obtidos ou buscar se necessário)
    base_url = "https://hmlzaffari.myvtex.com"
    if sku_data:
        merchandise_group = sku_data.get("merchandise_group")
        teste += f"process_product_replacement: merchandise_group_obtido_da_api={merchandise_group} - "
    else:
        # Se não tiver os dados, buscar apenas o Merchandise_Group
        merchandise_group = get_sku_merchandise_group(sku_id, engine)
        teste += f"process_product_replacement: merchandise_group_buscado_separadamente={merchandise_group} - "
    
    if not merchandise_group:
        # Se não encontrar Merchandise_Group, registrar SKU como processada e retornar erro
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            teste += f"process_product_replacement: ANTES_SALVAR_SKU_SEM_MERCH_GROUP sku_id={sku_id} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
            save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=current_order_id, teste=teste)
            teste += f"process_product_replacement: DEPOIS_SALVAR_SKU_SEM_MERCH_GROUP save_success={save_success} sku_id={sku_id} - "
            
            # Verificar se o SKU foi realmente salvo fazendo uma leitura
            if save_success and urn:
                try:
                    contact_data_verify, _ = get_weni_contact_robust(phone, engine) if phone else (None, None)
                    if contact_data_verify:
                        processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                        sku_salvo = str(sku_id) in processed_skus_verify
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SEM_MERCH_GROUP sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                    else:
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SEM_MERCH_GROUP contact_data_verify=None - "
                except Exception as e:
                    teste += f"process_product_replacement: ERRO_VERIFICACAO_SALVAMENTO_SEM_MERCH_GROUP={str(e)} - "
            else:
                teste += f"process_product_replacement: NAO_VERIFICOU_SALVAMENTO_SEM_MERCH_GROUP save_success={save_success} urn={urn} - "
        else:
            teste += f"process_product_replacement: SKU_JA_PROCESSADO_SEM_MERCH_GROUP sku_id={sku_id} processed_skus={processed_skus} - "
        return {
            "type": replacement_method,
            "error": f"Merchandise_Group não encontrado para SKU {sku_id}",
            "remove": True,
            "produto_antigo": produto_antigo,
            "teste": teste,
        }
    
    # Buscar produtos similares usando Merchandise_Group (API 2)
    products_structured, has_products_less_or_equal, found_but_rejected_by_20, teste = search_products_by_merchandise_group(
        merchandise_group, original_price, base_url, engine, original_sku_id=sku_id, original_quantity=quantity, teste=teste
    )

    teste += "products_structured: " + str(products_structured) + " - "
    teste += f"process_product_replacement: has_products_less_or_equal={has_products_less_or_equal} found_but_rejected_by_20={found_but_rejected_by_20} - "
    #products_structured = None
    
    if not products_structured:
        # Registrar SKU como processada mesmo sem encontrar produtos
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            teste += f"process_product_replacement: ANTES_SALVAR_SKU sku_id={sku_id} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
            save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=current_order_id, teste=teste)
            teste += f"process_product_replacement: DEPOIS_SALVAR_SKU save_success={save_success} sku_id={sku_id} - "
            
            # Verificar se o SKU foi realmente salvo fazendo uma leitura
            if save_success and urn:
                try:
                    contact_data_verify, _ = get_weni_contact_robust(phone, engine) if phone else (None, None)
                    if contact_data_verify:
                        processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                        sku_salvo = str(sku_id) in processed_skus_verify
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                    else:
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO contact_data_verify=None - "
                except Exception as e:
                    teste += f"process_product_replacement: ERRO_VERIFICACAO_SALVAMENTO={str(e)} - "
            else:
                teste += f"process_product_replacement: NAO_VERIFICOU_SALVAMENTO save_success={save_success} urn={urn} - "
        else:
            teste += f"process_product_replacement: SKU_JA_PROCESSADO sku_id={sku_id} processed_skus={processed_skus} - "
        
        # Determinar mensagem de erro baseado em se encontrou produtos mas todos foram rejeitados
        error_message = "SUBST NAO ADIC. - VALOR ACIMA" if found_but_rejected_by_20 else "SUBST NAO ADIC. - SEM OPCOES"
        teste += f"process_product_replacement: error_message={error_message} - "
        
        return {
            "type": replacement_method,
            "error": error_message,
            "remove": True,
            "produto_antigo": produto_antigo,
            "teste": teste,
        }

    
    # Para replacementBySimilar: se só temos produtos com valor total <= original,
    # o padrão é não falar com o cliente (auto-substituição). Esse valor ainda pode
    # ser ajustado mais adiante com base no preço unitário do substituto escolhido.
    if products_structured and has_products_less_or_equal == False:
        talk_to_client = False

    # Simular carrinho
    products_with_stock, teste = cart_simulation(
        base_url=base_url,
        products_details=products_structured,
        seller="1",
        quantity=quantity,
        country="BRA",
        engine=engine,
        teste=teste
    )

    if not products_with_stock:
        # Registrar SKU como processada
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            teste += f"process_product_replacement: ANTES_SALVAR_SKU_SEM_ESTOQUE sku_id={sku_id} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
            save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=current_order_id, teste=teste)
            teste += f"process_product_replacement: DEPOIS_SALVAR_SKU_SEM_ESTOQUE save_success={save_success} sku_id={sku_id} - "
            
            # Verificar se o SKU foi realmente salvo fazendo uma leitura
            if save_success and urn:
                try:
                    contact_data_verify, _ = get_weni_contact_robust(phone, engine) if phone else (None, None)
                    if contact_data_verify:
                        processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                        sku_salvo = str(sku_id) in processed_skus_verify
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SEM_ESTOQUE sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                    else:
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SEM_ESTOQUE contact_data_verify=None - "
                except Exception as e:
                    teste += f"process_product_replacement: ERRO_VERIFICACAO_SALVAMENTO_SEM_ESTOQUE={str(e)} - "
            else:
                teste += f"process_product_replacement: NAO_VERIFICOU_SALVAMENTO_SEM_ESTOQUE save_success={save_success} urn={urn} - "
        else:
            teste += f"process_product_replacement: SKU_JA_PROCESSADO_SEM_ESTOQUE sku_id={sku_id} processed_skus={processed_skus} - "
        return {
            "type": replacement_method,
            "error": "SUBST NAO ADIC. - SEM OPCOES",
            "remove": True,
            "produto_antigo": produto_antigo,
            "teste": teste,
        }

    
    # Filtrar produtos estruturados com estoque
    # Converter todos os sku_ids para string para comparação consistente
    sku_ids_with_stock = {str(product.get("sku", "")) for product in products_with_stock if product.get("sku")}
    products_structured_with_stock = {}
    
    for product_name_vtex, product_data in products_structured.items():
        sku_candidate = str(product_data.get("sku", "") or "")
        #price_candidate = product_data.get("price", 0)

        if sku_candidate in sku_ids_with_stock:
            products_structured_with_stock[product_name_vtex] = product_data

    
    # Escolher produtos com preço mais próximo - até 3 para contactConfirm, 1 para replacementBySimilar
    max_products = 3 if replacement_method == "contactConfirm" else 1
    chosen_products, teste = select_closest_products(
        products_structured_with_stock, original_price, max_products, teste=teste
    )

    
    if not chosen_products:
        # Registrar SKU como processada
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            teste += f"process_product_replacement: ANTES_SALVAR_SKU_SEM_CHOSEN sku_id={sku_id} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
            save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=current_order_id, teste=teste)
            teste += f"process_product_replacement: DEPOIS_SALVAR_SKU_SEM_CHOSEN save_success={save_success} sku_id={sku_id} - "
            
            # Verificar se o SKU foi realmente salvo fazendo uma leitura
            if save_success and urn:
                try:
                    contact_data_verify, _ = get_weni_contact_robust(phone, engine) if phone else (None, None)
                    if contact_data_verify:
                        processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                        sku_salvo = str(sku_id) in processed_skus_verify
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SEM_CHOSEN sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                    else:
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SEM_CHOSEN contact_data_verify=None - "
                except Exception as e:
                    teste += f"process_product_replacement: ERRO_VERIFICACAO_SALVAMENTO_SEM_CHOSEN={str(e)} - "
            else:
                teste += f"process_product_replacement: NAO_VERIFICOU_SALVAMENTO_SEM_CHOSEN save_success={save_success} urn={urn} - "
        else:
            teste += f"process_product_replacement: SKU_JA_PROCESSADO_SEM_CHOSEN sku_id={sku_id} processed_skus={processed_skus} - "
        return {
            "type": replacement_method,
            "error": "SUBST NAO ADIC. - VALOR ACIMA",
            "remove": True,
            "produto_antigo": produto_antigo,
            "teste": teste,
        }

    
    # Atualizar Weni com SKU processada (só adicionar se não existir)
    if sku_id not in processed_skus:
        updated_skus = processed_skus + [sku_id]
        teste += f"process_product_replacement: ANTES_SALVAR_SKU_SUCESSO sku_id={sku_id} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
        save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=current_order_id, teste=teste)
        teste += f"process_product_replacement: DEPOIS_SALVAR_SKU_SUCESSO save_success={save_success} sku_id={sku_id} - "
        
        # Verificar se o SKU foi realmente salvo fazendo uma leitura
        if save_success and urn:
            try:
                contact_data_verify, _ = get_weni_contact_robust(phone, engine) if phone else (None, None)
                if contact_data_verify:
                    processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                    sku_salvo = str(sku_id) in processed_skus_verify
                    teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SUCESSO sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                else:
                    teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_SUCESSO contact_data_verify=None - "
            except Exception as e:
                teste += f"process_product_replacement: ERRO_VERIFICACAO_SALVAMENTO_SUCESSO={str(e)} - "
        else:
            teste += f"process_product_replacement: NAO_VERIFICOU_SALVAMENTO_SUCESSO save_success={save_success} urn={urn} - "
    else:
        teste += f"process_product_replacement: SKU_JA_PROCESSADO_SUCESSO sku_id={sku_id} processed_skus={processed_skus} - "
    

    # Preparar resultado
    # REGRA GLOBAL (reaplicada aqui):
    #   - O valor total do pedido NUNCA deve ultrapassar o valor original.
    #   - O preço unitário do substituto pode ser até 20% maior (já filtrado em search_products_by_merchandise_group).
    #   - Se o substituto for mais barato ou igual, mantemos a quantidade igual à do item original.
    #   - Se for mais caro (até 20%), reduzimos a quantidade para caber no total original.
    # Limitar a 3 casas decimais
    original_total = round(float(original_price or 0) * float(quantity or 0), 3)
    produtos_escolhidos = []
    for produto in chosen_products:
        preco_sub = float(produto.get("price", 0) or 0)
        unit_multiplier = float(produto.get("unit_multiplier", 1.0))
        measurement_unit = str(produto.get("measurement_unit", "un") or "un").lower()

        if preco_sub <= 0:
            continue  # segurança contra divisão por zero

        # Se o preço é menor ou igual ao original, manter a quantidade igual à do item original
        if preco_sub <= float(original_price or 0):
            # Quantidade igual à quantidade original
            qty_sub = round(float(quantity or 0), 3)
            valor_total_final = round(preco_sub * qty_sub, 3)
        else:
            # Se o preço é maior que o original (mas até 20% maior), ajustar quantidade
            # para que o valor total NÃO ultrapasse o valor original.
            # Para produtos por unidade, a quantidade deve ser inteira;
            # para produtos por peso (kg), permitimos casas decimais.
            qty_ideal = original_total / preco_sub
            if quantity < 1 or measurement_unit == "kg":
                # Produtos vendidos por peso (kg): manter casas decimais
                qty_sub = round(qty_ideal, 3)
            else:
                # Produtos por unidade: usar inteiro (arredondar "para baixo")
                qty_sub = int(qty_ideal)
                if qty_sub <= 0:
                    qty_sub = 1

            valor_total_final = round(preco_sub * qty_sub, 3)

            # Se ainda ultrapassar devido a arredondamentos, reduzir proporcionalmente
            if valor_total_final > original_total and qty_sub > 0:
                # Reduzir em 0.001 para produtos por peso, ou 1 unidade para produtos por unidade
                reduction = 0.001 if (quantity < 1 or measurement_unit == "kg") else 1.0
                qty_sub = round(max(0, qty_sub - reduction), 3 if (quantity < 1 or measurement_unit == "kg") else 0)
                valor_total_final = round(preco_sub * qty_sub, 3)
        
        teste += f"produtos_escolhidos: SKU={produto['sku_id']} preco={preco_sub} qty={qty_sub} valor_total={valor_total_final} original_total={original_total} - "
        
        # Se após os ajustes a quantidade for menor que o mínimo aceitável, pular este produto
        # Para produtos por peso (quantity < 1), aceitar quantidades decimais >= 0.001
        # Para produtos por unidade (quantity >= 1), aceitar apenas >= 1
        min_qty_threshold = 0.001 if quantity < 1 else 1.0
        if qty_sub < min_qty_threshold:
            teste += f"produtos_escolhidos: SKU={produto['sku_id']} REJEITADO (qty={qty_sub} < {min_qty_threshold}, não cabe no limite original_total={original_total}) - "
            continue
        
        # Validação final: garantir que o valor total NUNCA ultrapasse o original
        if valor_total_final > original_total:
            teste += f"produtos_escolhidos: SKU={produto['sku_id']} REJEITADO (valor_total={valor_total_final} > original_total={original_total}) - "
            continue

        # Converter quantidade para unidades APENAS para cart_simulation (API requer unidades inteiras).
        # Para demais integrações (Zaffari, Weni, WhatsApp), usar sempre quantity em kg (float).
        quantity_in_units_for_cart_sim = convert_quantity_to_units(qty_sub, unit_multiplier, measurement_unit)
        
        produtos_escolhidos.append(
            {
                "sku": produto["sku_id"],
                "name": produto["name"],
                "price": preco_sub,
                # Quantidade principal em kg (ponto flutuante) para todas as integrações
                "quantity": qty_sub,
                # Quantidade em unidades usada exclusivamente dentro de cart_simulation
                "quantity_in_units_for_cart_sim": quantity_in_units_for_cart_sim,
                "unit_multiplier": unit_multiplier,  # Para conversões futuras
                "measurement_unit": measurement_unit,  # "kg" ou "un"
                "image_url": produto.get("image_url"),
            }
        )

        teste += f"produtos_escolhidos: sku={produto['sku_id']} - name={produto['name']} - price={preco_sub} - quantity={qty_sub} - "
    
    
    # Se nenhum produto passou na validação de valor total, retornar erro
    if not produtos_escolhidos:
        # Registrar SKU como processada
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            teste += f"process_product_replacement: ANTES_SALVAR_SKU_VALIDACAO_FINAL sku_id={sku_id} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
            save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=current_order_id, teste=teste)
            teste += f"process_product_replacement: DEPOIS_SALVAR_SKU_VALIDACAO_FINAL save_success={save_success} sku_id={sku_id} - "
            
            # Verificar se o SKU foi realmente salvo fazendo uma leitura
            if save_success and urn:
                try:
                    contact_data_verify, _ = get_weni_contact_robust(phone, engine) if phone else (None, None)
                    if contact_data_verify:
                        processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                        sku_salvo = str(sku_id) in processed_skus_verify
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_VALIDACAO_FINAL sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                    else:
                        teste += f"process_product_replacement: VERIFICACAO_SALVAMENTO_VALIDACAO_FINAL contact_data_verify=None - "
                except Exception as e:
                    teste += f"process_product_replacement: ERRO_VERIFICACAO_SALVAMENTO_VALIDACAO_FINAL={str(e)} - "
            else:
                teste += f"process_product_replacement: NAO_VERIFICOU_SALVAMENTO_VALIDACAO_FINAL save_success={save_success} urn={urn} - "
        else:
            teste += f"process_product_replacement: SKU_JA_PROCESSADO_VALIDACAO_FINAL sku_id={sku_id} processed_skus={processed_skus} - "
        return {
            "type": replacement_method,
            "error": "SUBST NAO ADIC. - VALOR ACIMA",
            "remove": True,
            "produto_antigo": produto_antigo,
            "teste": teste,
        }
    
    # Ajustar regra de falar com o cliente com base no aumento de preço unitário:
    #   - Para replacementBySimilar: só disparar flow se aumento >= 20% no preço unitário
    #     → talk_to_client = True → disparar fluxo Weni / WhatsApp para o cliente escolher.
    #   - Se for mais barato, igual, ou aumento < 20%, pode ser auto-substituído (talk_to_client = False),
    #     pois o valor total já está garantido como <= original ou o aumento é aceitável.
    try:
        if produtos_escolhidos:
            first_choice = produtos_escolhidos[0]
            preco_sub_escolhido = float(first_choice.get("price", 0) or 0)
            preco_original = float(original_price or 0)
            if replacement_method == "replacementBySimilar":
                if preco_original > 0:
                    percentual_aumento = ((preco_sub_escolhido - preco_original) / preco_original) * 100
                    if percentual_aumento >= 20.0:
                        talk_to_client = True
                        teste += f"process_product_replacement: preco_sub_escolhido={preco_sub_escolhido} > preco_original={preco_original} aumento={percentual_aumento:.2f}% >= 20% => talk_to_client=True - "
                    else:
                        talk_to_client = False
                        teste += f"process_product_replacement: preco_sub_escolhido={preco_sub_escolhido} vs preco_original={preco_original} aumento={percentual_aumento:.2f}% < 20% => talk_to_client=False - "
                else:
                    # Preço original inválido: tratar como auto-substituição
                    talk_to_client = False
                    teste += f"process_product_replacement: preco_original_invalido={preco_original} => talk_to_client=False - "
            else:
                # Para outros métodos (contactConfirm), manter lógica anterior se necessário
                if preco_sub_escolhido > preco_original:
                    talk_to_client = True
                    teste += f"process_product_replacement: preco_sub_escolhido={preco_sub_escolhido} > preco_original={preco_original} => talk_to_client=True - "
                else:
                    talk_to_client = False
                    teste += f"process_product_replacement: preco_sub_escolhido={preco_sub_escolhido} <= preco_original={preco_original} => talk_to_client=False - "
    except Exception as e:
        # Em caso de erro, manter valor anterior de talk_to_client e apenas logar
        teste += f"process_product_replacement: erro_ajuste_talk_to_client={e} - "

    return {
        "type": replacement_method,
        "talk_to_client": talk_to_client,
        "produtos_escolhidos": produtos_escolhidos,
        "produto_antigo": produto_antigo,  # Usar o dicionário completo que já tem todos os campos incluindo "unit"
        "remove": False,
        "teste": teste
    }
    

def get_fila_from_codeaction(numero_pedido, engine):
    """
    Faz GET na fila do Google Sheets através do CodeAction.
    Retorna o primeiro JSON da fila de um pedido específico sem remover.
    
    Args:
        numero_pedido (str): Número do pedido
        engine: Engine object para logging
    
    Returns:
        dict: JSON do primeiro item da fila do pedido ou None se a fila estiver vazia
    """
    try:
        # URL do CodeAction de fila
        codeaction_url = "https://code-actions.weni.ai/action/endpoint/692034097a06b1c824249d9d?numeroPedido=" + str(numero_pedido)
        
        payload = {
            "acao": "get",
        }
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        
        # CodeAction espera POST, não GET
        response = requests.post(codeaction_url, json=payload, headers=headers, timeout=30)
        
        # Verificar se a resposta tem conteúdo
        response_text = response.text.strip()
        
        response.raise_for_status()
        
        result = response.json()
        
        status = result.get("status")
        
        if status == "ok":
            primeiro_json = result.get("primeiroJson")
            return primeiro_json
        elif status in ["vazia", "fila_vazia", "nao_encontrado"]:
            mensagem = result.get("mensagem", "Sem mensagem")
            engine.log.debug(f"ℹ️ [GET_FILA] Fila vazia ou pedido não encontrado - Status: {status}, Mensagem: {mensagem}, Pedido: {numero_pedido}")
            return None
        else:
            mensagem = result.get("mensagem", "Erro desconhecido")
            engine.log.debug(f"⚠️ [GET_FILA] Erro ao fazer GET na fila - Status: {status}, Mensagem: {mensagem}")
            return None
            
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_response = e.response.json()
                error_msg = f"{error_msg} | Response: {json.dumps(error_response, ensure_ascii=False)[:200]}"
            except:
                error_msg = f"{error_msg} | Response text: {e.response.text[:200]}"
        engine.log.debug(f"❌ [GET_FILA] Erro na requisição GET: {error_msg}")
        return None
    except json.JSONDecodeError as e:
        engine.log.debug(f"❌ [GET_FILA] Erro ao fazer parse da resposta GET: {e}")
        engine.log.debug(f"❌ [GET_FILA] Response text que causou erro: {response_text[:500] if 'response_text' in locals() else 'N/A'}")
        return None
    except Exception as e:
        engine.log.debug(f"❌ [GET_FILA] Erro inesperado no GET: {e}")
        import traceback
        engine.log.debug(f"❌ [GET_FILA] Traceback: {traceback.format_exc()}")
        return None


def push_fila_from_codeaction(numero_pedido, json_data, engine):
    """
    Faz PUSH na fila do Google Sheets através do CodeAction.
    Adiciona um JSON à fila de um pedido específico.
    
    Args:
        numero_pedido (str): Número do pedido
        json_data (dict): JSON a ser adicionado na fila
        engine: Engine object para logging
    
    Returns:
        dict: {"success": bool, "message": str, "error": str|None}
    """
    try:
        # URL do CodeAction de fila
        codeaction_url = "https://code-actions.weni.ai/action/endpoint/692034097a06b1c824249d9d?numeroPedido=" + str(numero_pedido)
        
        payload = {
            "acao": "push",
            "json": json_data
        }
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        
        response = requests.post(codeaction_url, json=payload, headers=headers, timeout=30)
        
        # Verificar status antes de fazer raise_for_status para capturar resposta de erro
        if response.status_code >= 400:
            return {
                "success": False,
                "message": f"Erro HTTP {response.status_code} ao adicionar JSON na fila"
            }
        
        response.raise_for_status()
        result = response.json()
        
        if result.get("status") == "ok":
            return {
                "success": True,
                "message": "JSON adicionado à fila com sucesso"
            }
        else:
            return {
                "success": False,
                "message": result.get("mensagem", "Erro desconhecido ao fazer PUSH na fila")
            }
            
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "message": str(e)
        }
    except json.JSONDecodeError as e:
        return {
            "success": False,
            "message": f"Erro ao processar resposta: {str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Erro inesperado: {str(e)}"
        }


def pop_fila_from_codeaction(numero_pedido, engine):
    """
    Faz POP na fila do Google Sheets através do CodeAction.
    Remove e retorna o primeiro JSON da fila de um pedido específico.
    
    Args:
        numero_pedido (str): Número do pedido
        engine: Engine object para logging
    
    Returns:
        dict: {"success": bool, "message": str, "jsonRemovido": dict|None, "error": str|None}
    """
    try:
        # URL do CodeAction de fila
        codeaction_url = "https://code-actions.weni.ai/action/endpoint/692034097a06b1c824249d9d?numeroPedido=" + str(numero_pedido)
        
        payload = {
            "acao": "pop",
            "numeroPedido": str(numero_pedido)
        }
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        # Timeout reduzido para 30s para evitar timeout do contexto de requisição (era 90s)
        # O pop_fila pode levar até ~72s com 8 tentativas (2s, 4s, 6s, 8s, 10s, 12s, 14s, 16s)
        # Adicionando margem de segurança de 18s para garantir conclusão mesmo com delays do Google Sheets
        response = requests.post(codeaction_url, json=payload, headers=headers, timeout=30)
        
        # Verificar status antes de fazer raise_for_status para capturar resposta de erro
        if response.status_code >= 400:
            return {
                "success": False,
                "message": f"Erro HTTP {response.status_code} ao remover JSON da fila",
                "jsonRemovido": None
            }
        
        response.raise_for_status()
        result = response.json()
        
        if result.get("status") == "ok":
            json_removido = result.get("jsonRemovido")
            return {
                "success": True,
                "message": "JSON removido da fila com sucesso",
                "jsonRemovido": json_removido
            }
        elif result.get("status") in ["vazio", "vazio_ou_inexistente", "fila_vazia", "nao_encontrado"]:
            return {
                "success": True,
                "message": result.get("mensagem", "Fila vazia ou pedido não encontrado"),
                "jsonRemovido": None
            }
        else:
            return {
                "success": False,
                "message": result.get("mensagem", "Erro desconhecido ao fazer POP na fila"),
                "jsonRemovido": None
            }
            
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "message": str(e),
            "jsonRemovido": None
        }
    except json.JSONDecodeError as e:
        return {
            "success": False,
            "message": f"Erro ao processar resposta: {str(e)}",
            "jsonRemovido": None
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Erro inesperado: {str(e)}",
            "jsonRemovido": None
        }


def contact_length_of_items(contact_data, engine, request, length_of_items=None, urn=None, teste=None):
    """
    Atualiza ou extrai a quantidade de itens no pedido do cliente

    Args:
        contact_data (dict): Dados do contato do cliente
        engine (Engine): Engine object para logging
        request (str): "update" para atualizar, "extract" para extrair
        length_of_items (int): Quantidade de itens no pedido
        urn (str): URN do contato do cliente
    """

    # Preparar string de log local
    teste_local = teste if isinstance(teste, str) else ""
    teste_local += f"contact_length_of_items: request={request} length_of_items_param={length_of_items} urn={urn} - "

    if request == "update":
        # Atualizar a quantidade de itens no pedido do cliente
        if urn is None or length_of_items is None:
            msg = "Error: urn e length_of_items são obrigatórios para update em contact_length_of_items"
            teste_local += msg + " - "
            return (False, teste_local) if teste is not None else False
        try:
            url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
            headers = {
                "Authorization": "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70",  #token do projeto
                "Content-Type": "application/json",
            }

            fields = {"items_length": length_of_items}

            payload = {"name": contact_data.get("name"), "fields": fields}
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            success = response.status_code == 200
            teste_local += f"contact_length_of_items:update status={response.status_code} success={success} - "
            return (success, teste_local) if teste is not None else success
        except requests.exceptions.RequestException as e:
            engine.log.debug(f"Error updating Weni contact: {e}")
            teste_local += f"contact_length_of_items:update request_error={e} - "
            return (False, teste_local) if teste is not None else False
        except Exception as e:
            engine.log.debug(f"Error updating Weni contact: {e}")
            teste_local += f"contact_length_of_items:update unexpected_error={e} - "
            return (False, teste_local) if teste is not None else False
        
    # request == "extract" → apenas ler valor salvo no contato
    try:
        if contact_data and "fields" in contact_data:

            # Campo salvo em Weni: 'items_length'
            raw_items_length = contact_data["fields"].get("items_length")
            if raw_items_length is None or raw_items_length == "":
                items_length = 0
                teste_local += "contact_length_of_items:extract items_length not set, defaulting to 0 - "
            else:
                try:
                    items_length = int(raw_items_length)
                    teste_local += f"contact_length_of_items:extract items_length_raw={raw_items_length} parsed={items_length} - "
                except (ValueError, TypeError) as e:
                    # Se vier algo inesperado, loga e assume 0
                    teste_local += f"contact_length_of_items:extract parse_error for '{raw_items_length}' => {e}, using 0 - "
                    items_length = 0
            return (items_length, teste_local) if teste is not None else items_length
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        if engine:
            teste_local += f"contact_length_of_items:extract error={e} - "
    return (0, teste_local) if teste is not None else 0


def contact_removed_count(contact_data, engine, request, removed_count=None, urn=None, teste=None):
    """
    Atualiza ou extrai a quantidade de produtos REMOVED do pedido do cliente

    Args:
        contact_data (dict): Dados do contato do cliente
        engine (Engine): Engine object para logging
        request (str): "update" para atualizar, "extract" para extrair
        removed_count (int): Quantidade de produtos REMOVED
        urn (str): URN do contato do cliente
        teste (str, optional): String para acumular logs de debug
    
    Returns:
        tuple ou int: (removed_count, teste_atualizado) se teste não for None, senão apenas removed_count
    """
    # Preparar string de log local
    teste_local = teste if isinstance(teste, str) else ""
    teste_local += f"contact_removed_count: request={request} removed_count_param={removed_count} urn={urn} - "

    if request == "update":
        # Atualizar a quantidade de produtos REMOVED no contato do cliente
        if urn is None or removed_count is None:
            msg = "Error: urn e removed_count são obrigatórios para update em contact_removed_count"
            teste_local += msg + " - "
            return (False, teste_local) if teste is not None else False
        try:
            url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
            headers = {
                "Authorization": "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70",  #token do projeto
                "Content-Type": "application/json",
            }

            fields = {"removed_count": removed_count}

            payload = {"name": contact_data.get("name"), "fields": fields}
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            success = response.status_code == 200
            teste_local += f"contact_removed_count:update status={response.status_code} success={success} - "
            return (success, teste_local) if teste is not None else success
        except requests.exceptions.RequestException as e:
            engine.log.debug(f"Error updating Weni contact removed_count: {e}")
            teste_local += f"contact_removed_count:update request_error={e} - "
            return (False, teste_local) if teste is not None else False
        except Exception as e:
            engine.log.debug(f"Error updating Weni contact removed_count: {e}")
            teste_local += f"contact_removed_count:update unexpected_error={e} - "
            return (False, teste_local) if teste is not None else False
        
    # request == "extract" → apenas ler valor salvo no contato
    try:
        if contact_data and "fields" in contact_data:

            # Campo salvo em Weni: 'removed_count'
            raw_removed_count = contact_data["fields"].get("removed_count")
            if raw_removed_count is None or raw_removed_count == "":
                removed_count_saved = 0
                teste_local += "contact_removed_count:extract removed_count not set, defaulting to 0 - "
            else:
                try:
                    removed_count_saved = int(raw_removed_count)
                    teste_local += f"contact_removed_count:extract removed_count_raw={raw_removed_count} parsed={removed_count_saved} - "
                except (ValueError, TypeError) as e:
                    # Se vier algo inesperado, loga e assume 0
                    teste_local += f"contact_removed_count:extract parse_error for '{raw_removed_count}' => {e}, using 0 - "
                    removed_count_saved = 0
            return (removed_count_saved, teste_local) if teste is not None else removed_count_saved
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        if engine:
            teste_local += f"contact_removed_count:extract error={e} - "
    return (0, teste_local) if teste is not None else 0

    

def Run(engine):
    teste = "inicio - "
    #####################################################################
    # Essa etapa é a verificação para disparar flows (WhatsApp Flow via Meta),
    # independente do fluxo principal de substituição.
    # Aceita tanto via params (GET) quanto via body (POST).
    #####################################################################

    disparar_fluxo_weni = None
    phone = None
    produtos_escolhidos = None
    produto_antigo = None
    start_flow = False
    
    # Log inicial para debug
    try:
        body_preview = str(engine.body)[:200] if hasattr(engine, 'body') and engine.body else "None"
        teste += f"Run_inicio body_preview={body_preview} - "
    except Exception:
        teste += "Run_inicio body_preview=erro_ao_ler - "
    
    # Tentar ler do body primeiro (POST com JSON)
    try:
        if engine.body:
            # Se o body for string, tentar fazer parse
            if isinstance(engine.body, str):
                # Tentar sanitizar escapes inválidos antes de fazer parse
                sanitized_body = sanitize_invalid_json_escapes(engine.body)
                try:
                    body_data = json.loads(sanitized_body)
                except json.JSONDecodeError:
                    # Se falhar, tentar parse direto (pode ser que já esteja parseado)
                    try:
                        body_data = json.loads(engine.body)
                    except json.JSONDecodeError:
                        teste += f"body_parse_erro_json body_preview={engine.body[:200]} - "
                        body_data = None
            else:
                body_data = engine.body
            
            if isinstance(body_data, dict):
                disparar_fluxo_weni = body_data.get("disparar_flow")
                teste += f"body_detectado={bool(disparar_fluxo_weni)} - "
                if disparar_fluxo_weni == "true" or disparar_fluxo_weni is True:
                    phone = body_data.get("phone")
                    produto_antigo_raw = body_data.get("produto_antigo")
                    
                    # produto_antigo continua vindo no body/params
                    if isinstance(produto_antigo_raw, str):
                        # Tentar sanitizar antes de fazer parse
                        sanitized_produto = sanitize_invalid_json_escapes(produto_antigo_raw)
                        try:
                            produto_antigo = json.loads(sanitized_produto)
                        except json.JSONDecodeError:
                            try:
                                produto_antigo = json.loads(produto_antigo_raw)
                            except json.JSONDecodeError as e:
                                teste += f"body_produto_antigo_parse_erro={str(e)} - "
                                produto_antigo = None
                    else:
                        produto_antigo = produto_antigo_raw
                    teste += f"body_parse_sucesso phone={bool(phone)} produto_antigo={bool(produto_antigo)} - "
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        teste += f"body_parse_erro={str(e)} - "
    
    # Fallback: tentar ler de params (GET ou query string)
    if not disparar_fluxo_weni:
        disparar_fluxo_weni = engine.params.get("disparar_flow") if hasattr(engine, "params") else None
        teste += f"fallback_params_disparar={disparar_fluxo_weni} - "
    
    if disparar_fluxo_weni == "true" or disparar_fluxo_weni is True:
        # Se não pegou do body, tentar params
        if not phone and hasattr(engine, "params"):
            phone = engine.params.get("phone")
            teste += f"param_phone={'ok' if phone else 'vazio'} - "
        if not produto_antigo and hasattr(engine, "params"):
            produto_antigo_raw = engine.params.get("produto_antigo")
            if produto_antigo_raw:
                try:
                    produto_antigo = json.loads(produto_antigo_raw) if isinstance(produto_antigo_raw, str) else produto_antigo_raw
                    teste += "param_produto_antigo_ok - "
                except (json.JSONDecodeError, TypeError):
                    teste += "param_produto_antigo_parse_erro - "

        # Buscar produtos_escolhidos salvos nas variáveis de contato da Weni
        if phone:
            contact_data, urn = get_weni_contact_robust(phone, engine)
            teste += f"flow_disparo: contact_data_obtido={bool(contact_data)} urn={urn} - "
            raw_produtos = []

            if contact_data and isinstance(contact_data, dict):
                contact_fields = contact_data.get("fields") or {}

                # Coletar chunks no padrão items_partN (mesma lógica do recebedor.py)
                chunk_keys = sorted(
                    k for k in contact_fields.keys() if str(k).startswith("items_part")
                )
                teste += f"flow_disparo: chunk_keys={chunk_keys if chunk_keys else 'nenhum'} - "

                for key in chunk_keys:
                    raw_chunk = contact_fields.get(key)
                    if isinstance(raw_chunk, str) and raw_chunk.strip():
                        sanitized_chunk = sanitize_invalid_json_escapes(raw_chunk)
                        try:
                            parsed_chunk = json.loads(sanitized_chunk)
                        except json.JSONDecodeError as e:
                            teste += f"flow_disparo: erro_parse_{key}={e} - "
                            continue

                        if isinstance(parsed_chunk, dict):
                            chunk_items = parsed_chunk.get("items")
                            if isinstance(chunk_items, list):
                                raw_produtos.extend(chunk_items)
                                teste += f"flow_disparo: {key}_itens_dict={len(chunk_items)} - "
                            else:
                                teste += f"flow_disparo: {key}_sem_lista_items_dict - "
                        elif isinstance(parsed_chunk, list):
                            raw_produtos.extend(parsed_chunk)
                            teste += f"flow_disparo: {key}_itens_list={len(parsed_chunk)} - "
                        else:
                            teste += f"flow_disparo: {key}_tipo_inesperado={type(parsed_chunk)} - "
                    elif isinstance(raw_chunk, dict):
                        chunk_items = raw_chunk.get("items")
                        if isinstance(chunk_items, list):
                            raw_produtos.extend(chunk_items)
                            teste += f"flow_disparo: {key}_itens_dict_direto={len(chunk_items)} - "
                        else:
                            teste += f"flow_disparo: {key}_dict_sem_lista_items - "

            # Normalizar estrutura para o formato esperado por send_whatsapp_flow_after_weni
            if raw_produtos:
                produtos_normalizados = []
                for p in raw_produtos:
                    if not isinstance(p, dict):
                        continue
                    sku = p.get("sku") or p.get("id")
                    if not sku:
                        continue

                    # Quantidade base sempre em kg (float) quando aplicável
                    quantity_kg = float(p.get("quantity", 1) or 1)
                    unit_mult = float(p.get("unit_multiplier", 1.0) or 1.0)
                    measurement_unit = str(p.get("measurement_unit", "un") or "un").lower()

                    # Calcular quantity_in_units apenas para uso interno em cart_simulation
                    quantity_in_units_calc = None
                    if measurement_unit == "kg" and unit_mult > 0 and unit_mult != 1.0:
                        quantity_in_units_calc = round(quantity_kg / unit_mult, 3)

                    produtos_normalizados.append(
                        {
                            "sku": str(sku),
                            "name": p.get("name", ""),
                            "price": p.get("price", 0),
                            "quantity": quantity_kg,  # Quantidade em kg (ponto flutuante)
                            "quantity_in_units_for_cart_sim": quantity_in_units_calc,
                            "unit_multiplier": unit_mult,
                            "measurement_unit": measurement_unit,
                            "image_url": p.get("image_url", ""),
                        }
                    )
                produtos_escolhidos = produtos_normalizados
                teste += f"flow_disparo: produtos_escolhidos_recuperados={len(produtos_escolhidos)} - "
            else:
                teste += "flow_disparo: nenhum_produto_encontrado_nas_variaveis_de_contato - "

        # Validar dados antes de processar
        if phone and produtos_escolhidos and produto_antigo:
            teste += f"flow_dados_ok_enviando phone={bool(phone)} produtos_count={len(produtos_escolhidos) if produtos_escolhidos else 0} produto_antigo={bool(produto_antigo)} - "
            try:
                send_success = send_whatsapp_flow_after_weni(
                    phone=phone,
                    produtos_escolhidos=produtos_escolhidos,
                    produto_antigo=produto_antigo,
                    engine=engine,
                    flow_id="1584991322669299",
                )
                teste += f"send_whatsapp_flow_after_weni retornou={send_success} - "
                engine.result.set({
                    "status": "Success",
                    "message": "Fluxo WhatsApp enviado com sucesso",
                    "send_success": send_success,
                    "teste": teste
                }, status_code=200, content_type="json")
                return
            except Exception as e:
                teste += f"flow_erro_ao_enviar_whatsapp={str(e)} - "
                engine.result.set({
                    "status": "Error",
                    "error": f"Erro ao enviar fluxo WhatsApp: {str(e)}",
                    "teste": teste
                }, status_code=500, content_type="json")
                return
        else:
            teste += (
                f"flow_dados_faltando phone={bool(phone)} "
                f"prod_escolhidos={bool(produtos_escolhidos)} prod_antigo={bool(produto_antigo)} - "
            )
            # Verificar se produtos_escolhidos é lista vazia (diferente de None)
            produtos_count = len(produtos_escolhidos) if produtos_escolhidos else 0
            teste += f"flow_dados_faltando produtos_count={produtos_count} - "
            engine.result.set({
                "status": "Error",
                "error": "Parâmetros obrigatórios ausentes ou dados não encontrados nas variáveis de contato",
                "missing": {
                    "phone": not phone,
                    "produtos_escolhidos": not produtos_escolhidos or produtos_count == 0,
                    "produto_antigo": not produto_antigo
                },
                "produtos_count": produtos_count,
                "teste": teste,
            }, status_code=400, content_type="json")
            return
    else:
        teste += "flow_nao_disparado_flag_false - "

    #####################################################################
    # Essa etapa é a execução do processamento da substituição do produto
    # (replacementBySimilar, contactConfirm, noReplacement) e também faz
    # o controle de fila (CodeAction/Google Sheets) e de estado na Weni.
    #####################################################################

    first_contact = True # flag para indicar se é o primeiro contato com o cliente

    # Se client_reference vim como parametro, significa que a fila executou o primeiro JSON, deu pop e deve executar o próximo JSON
    client_reference_param = engine.params.get("client_reference")
    input_data = None
    
    if client_reference_param is not None:
        # Tentar fazer GET na fila primeiro
        if client_reference_param:
            # Fazer GET na fila do pedido específico
            json_da_fila = get_fila_from_codeaction(client_reference_param, engine)
            
            if json_da_fila:
                # Se encontrou JSON na fila, usar ele como input_data
                input_data = json_da_fila
                start_flow = True # Deve disparar fluxo Weni novamente
            else:
                # Se não encontrou JSON na fila, retornar erro
                engine.result.set({
                    "error": "Nenhum JSON encontrado na fila para processar",
                    "client_reference": str(client_reference_param)
                }, status_code=404, content_type="json")
                return
    else:
        # Se client_reference não vim como parametro, significa que é o primeiro contato com o cliente ou um novo JSON para fazer push na fila
        body = engine.body
        if body:
            try:
                body_data = json.loads(body) if isinstance(body, str) else body
                if isinstance(body_data, dict):
                    input_data = body_data
                    start_flow = False # se for um novo pedido, start_flow é atualizado posteriormente
                else:
                    engine.result.set({"error": "Body deve ser um objeto JSON válido"}, status_code=400, content_type="json")
                    return
            except (json.JSONDecodeError, TypeError) as e:
                engine.result.set({
                    "error": f"Erro ao processar body JSON: {str(e)}"
                }, status_code=400, content_type="json")
                return
        else:
            engine.result.set({"error": "body não encontrado"}, status_code=400, content_type="json")
            return

    # Validar se input_data foi definido
    if input_data is None:
        engine.result.set({
            "error": "input_data não foi definido corretamente"
        }, status_code=400, content_type="json")
        return
    
    # Obter detalhes do pedido VTEX
    client_reference = input_data.get("job", {}).get("client_reference", "")
    
    # Normalizar para o formato <numeros>-<dois_digitos>
    m = re.search(r"(\d+)-(\d{2})", client_reference)
    if m:
        client_reference = f"{m.group(1)}-{m.group(2)}"
    else:
        digits_only = re.sub(r"\D", "", client_reference)
        if len(digits_only) >= 3:
            client_reference = f"{digits_only[:-2]}-{digits_only[-2:]}"
        else:
            client_reference = digits_only

    if not client_reference:
        engine.result.set({"error": "client_reference não encontrado"}, status_code=400, content_type="json")
        return
    vtex_order = get_vtex_order_details(client_reference, engine)
    if not vtex_order:
        engine.result.set({"error": "Pedido não encontrado na VTEX"}, status_code=502, content_type="json")  # Bad Gateway - erro em API externa
        return

    #Capturando id do itens promocionais
    promotional_items = []
    rates_and_benefits = vtex_order.get("ratesAndBenefitsData", {})
    rate_and_benefits_identifiers = rates_and_benefits.get("rateAndBenefitsIdentifiers", [])
    
    for identifier in rate_and_benefits_identifiers:
        matched_params = identifier.get("matchedParameters", {})
        for_the_price_of = matched_params.get("forThePriceOf@Marketing")
        if for_the_price_of:
            promotional_items.append(for_the_price_of)
    
    teste = ""
    # Capturando o replacementMethod
    custom_apps = vtex_order.get("customData", {}).get("customApps", [])
    replacement_method = None
    for app in custom_apps:
        if app.get("id") == "order":
            replacement_method = app.get("fields", {}).get("replacementMethod")
            break
    
    type_of_process = ""
    #regra de remover 50% de produtos
    job_items = input_data.get("job", {}).get("job_items", [])
    removed_items = [item for item in job_items if item.get("status") == "REMOVED"]
    
    # Contar apenas SKUs únicos para evitar problemas com duplicatas
    # Um mesmo SKU pode aparecer múltiplas vezes (ex: item promocional), mas deve contar apenas 1 vez
    all_skus = [str(item.get("id")) for item in job_items if item.get("id")]
    unique_skus = list(dict.fromkeys(all_skus))  # Remove duplicatas mantendo ordem
    total_unique_skus = len(unique_skus)
    
    removed_ids = [str(item.get("id")) for item in removed_items if item.get("id")]
    unique_removed_skus = list(dict.fromkeys(removed_ids))  # Remove duplicatas mantendo ordem
    removed_count_unique = len(unique_removed_skus)
    
    # Calcular percentual baseado em SKUs únicos
    if total_unique_skus > 0 and (removed_count_unique / total_unique_skus) >= 0.5:
        percentual = (removed_count_unique / total_unique_skus) * 100
        teste += f"removed_count: {removed_count_unique} (unique) - total_skus: {total_unique_skus} (unique) - removed_count_unique / total_unique_skus: {percentual:.2f}% - "
        #quando acima de 50% de produtos removidos, processar como contactConfirm 
        type_of_process = "contactConfirm"
        teste += "50% removed - "



    # ======================================================================
    # Verificação global: todos os SKUs REMOVED deste pedido já processados?
    # - Usa contato da Weni para ler SKUs já processados (campo "sku").
    # - Se todos os REMOVED já estiverem na Weni, nada mais é feito.
    # ======================================================================
    if removed_items:
        recipient = input_data.get("job", {}).get("recipient", {})
        phone = recipient.get("phone_number", "")
        full_name = recipient.get("name", "")

        processed_skus = []
        if phone:
            contact_data, urn = get_weni_contact_robust(phone, engine)
            if contact_data:
                order_id_weni = get_order_id_from_weni(contact_data, engine)
                current_order_id = client_reference or ""

                if order_id_weni and order_id_weni != current_order_id:
                    # Pedido novo: considerar que não há SKUs processadas ainda
                    processed_skus = []
                else:
                    processed_skus = get_processed_skus_from_weni(contact_data, engine)

        removed_ids = [str(item.get("id")) for item in removed_items if item.get("id")]
        if removed_ids and processed_skus and all(rid in processed_skus for rid in removed_ids):
            # Todos os SKUs REMOVED deste pedido já foram processados anteriormente
            teste += f"verificacao_global: todas_skus_removed_ja_processadas removed_ids={removed_ids} processed_skus={processed_skus} - "
            
            # Se veio da fila, fazer pop para remover o item obsoleto da fila
            if client_reference_param is not None:
                teste += f"pop_fila: removendo_da_fila_todas_skus_ja_processadas pedido={client_reference} - "
                pop_result = pop_fila_from_codeaction(client_reference, engine)
                if pop_result.get("success"):
                    teste += f"pop_fila: SUCESSO pedido={client_reference} - "
                else:
                    teste += f"pop_fila: ERRO pedido={client_reference} mensagem={pop_result.get('message', 'Erro desconhecido')} - "
            
            engine.result.set(
                {
                    "Status": "Success",
                    "type": replacement_method,
                    "message": "Todas as SKUs REMOVED deste pedido já foram processadas anteriormente. Nenhuma nova ação será realizada.",
                    "teste": teste
                },
                status_code=409,  # Conflict - SKUs já processadas (conflito de estado)
                content_type="json",
            )
            return

    #####################################################################
    # Verificação: É um JSON com item novo?
    # - Compara length_of_job_items (qtde de itens no JSON atual) com
    #   length_of_items salvo na Weni (campo "items_length").
    # - Se length_of_job_items > length_of_weni, trata como "novo item adicionado"
    #   e apenas registra a notificação, sem processar substituição.
    #####################################################################

    # Garantir que phone está definido
    teste += f"verificacao_novo_item: inicio - "
    if not phone:
        recipient = input_data.get("job", {}).get("recipient", {})
        phone = recipient.get("phone_number", "")
        teste += f"verificacao_novo_item: phone_obtido_do_recipient={bool(phone)} - "
    
    job_items = input_data.get("job", {}).get("job_items", [])
    length_of_job_items = len(job_items)
    teste += f"verificacao_novo_item: length_of_job_items={length_of_job_items} - "
    
    if phone:
        contact_data, urn = get_weni_contact_robust(phone, engine)
        teste += f"verificacao_novo_item: contact_data_obtido={bool(contact_data)} urn={urn} - "
        length_of_weni, teste = contact_length_of_items(contact_data, engine, "extract", teste=teste)
        teste += f"verificacao_novo_item: length_of_weni_extraido={length_of_weni} - "
    else:
        teste += f"verificacao_novo_item: phone_nao_encontrado - "
        engine.result.set({
            "error": "phone não encontrado",
            "teste": teste
        }, status_code=400, content_type="json")
        return

    # Verificar se o pedido novo
    numero_pedido_weni = get_order_id_from_weni(contact_data, engine)
    teste += f"verificacao_novo_item: numero_pedido_weni={numero_pedido_weni} client_reference={client_reference} - "
    if numero_pedido_weni and numero_pedido_weni != client_reference:
        # Pedido novo: considerar que não há SKUs processadas
        teste += f"verificacao_novo_item: pedido_novo_detectado limpando_length_of_weni - "
        processed_skus = []
        _, teste = update_weni_contact(urn, full_name, processed_skus, engine, order_id=client_reference, teste=teste)
        _, teste = contact_length_of_items(contact_data, engine, "update", 0, urn, teste)
        _, teste = contact_removed_count(contact_data, engine, "update", 0, urn, teste)
        length_of_weni = 0
        teste += f"verificacao_novo_item: length_of_weni_resetado_para_0 removed_count_resetado_para_0 - "
    else:
        first_contact = False

    teste += f"verificacao_novo_item: comparacao length_of_job_items={length_of_job_items} > length_of_weni={length_of_weni} (ou 0) - "
    teste += f"verificacao_novo_item: condicao1={length_of_job_items > (length_of_weni or 0)} condicao2={length_of_weni is not None} condicao3={length_of_weni != 0} - "
    
    # VERIFICAÇÃO CRÍTICA: Antes de considerar como "novo item adicionado",
    # verificar se há itens REMOVED não processados. Se houver, processar mesmo que
    # length_of_job_items > length_of_weni (corrige o bug onde REMOVED eram ignorados
    # quando a contagem de itens aumentava)
    has_unprocessed_removed = False
    if removed_items and contact_data:
        # Obter SKUs processadas se ainda não foram obtidas
        if phone:
            # Verificar se é pedido novo
            order_id_weni_check = get_order_id_from_weni(contact_data, engine)
            current_order_id_check = client_reference or ""
            if order_id_weni_check and order_id_weni_check != current_order_id_check:
                processed_skus_check = []
            else:
                processed_skus_check = get_processed_skus_from_weni(contact_data, engine)
            
            # Verificar se há REMOVED não processados
            removed_ids_check = [str(item.get("id")) for item in removed_items if item.get("id")]
            unprocessed_removed = [rid for rid in removed_ids_check if rid and rid not in processed_skus_check]
            has_unprocessed_removed = len(unprocessed_removed) > 0
            teste += f"verificacao_novo_item: verificacao_removed_nao_processados unprocessed_count={len(unprocessed_removed)} unprocessed_skus={unprocessed_removed} has_unprocessed_removed={has_unprocessed_removed} - "
    
    if (length_of_job_items > (length_of_weni or 0)) and length_of_weni is not None and length_of_weni != 0:
        # Verificar se há REMOVED não processados antes de retornar como "novo item adicionado"
        if has_unprocessed_removed:
            # Há REMOVED não processados: continuar processamento mesmo que contagem tenha aumentado
            teste += f"verificacao_novo_item: REMOVED_NAO_PROCESSADOS_DETECTADOS continuando_processamento_mesmo_com_contagem_maior - "
        else:
            # Não há REMOVED não processados: tratar como "novo item adicionado"
            teste += f"verificacao_novo_item: NOVO_ITEM_DETECTADO - "
            teste += f"verificacao_novo_item: length_of_job_items={length_of_job_items} length_of_weni={length_of_weni} diferenca={length_of_job_items - length_of_weni} - "
            if contact_data and urn:
                _, teste = contact_length_of_items(contact_data, engine, "update", length_of_job_items, urn, teste)
                teste += f"verificacao_novo_item: length_of_items_atualizado_na_weni_para={length_of_job_items} - "
            else:
                teste += f"verificacao_novo_item: AVISO_contact_data_ou_urn_nao_disponivel_para_update - "
            
            teste += f"verificacao_novo_item: retornando_notificacao_novo_item - "
            engine.result.set({
                "Status": "Success",
                "type": replacement_method,
                "message": "Notificação recebida após novo item ser adicionado ao pedido",
                "teste": teste,
                "length_of_job_items": length_of_job_items,
                "length_of_weni": length_of_weni,
                "client_reference": client_reference
            }, status_code=200, content_type="json")
            return
    else:
        # Não é um novo item adicionado - pode ser pedido novo ou mesmo pedido
        if length_of_weni == 0:
            teste += f"verificacao_novo_item: NAO_E_NOVO_ITEM length_of_weni=0 (pedido_novo_ou_primeira_vez) continuando_processamento - "
        elif length_of_job_items <= length_of_weni:
            teste += f"verificacao_novo_item: NAO_E_NOVO_ITEM length_of_job_items={length_of_job_items} <= length_of_weni={length_of_weni} continuando_processamento - "
        else:
            teste += f"verificacao_novo_item: NAO_E_NOVO_ITEM condicao3_falhou (length_of_weni={length_of_weni} != 0) continuando_processamento - "

    # Atualizar sempre o length_of_items com o valor atual de job_items,
    # registrando logs na variável teste
    teste += f"verificacao_novo_item: atualizando_length_of_items_para={length_of_job_items} - "
    _, teste = contact_length_of_items(contact_data, engine, "update", length_of_job_items, urn, teste)
    teste += f"verificacao_novo_item: length_of_items_atualizado continuando_para_processamento_substituicao - "


    #####################################################################
    # Processar baseado no método
    #####################################################################
    # IMPORTANTE:
    # - process_result é usado mais abaixo para decidir se deve dar pop na fila.
    # - Em alguns cenários (ex: replacement_method inválido ou não configurado),
    #   o bloco principal não atribui nenhum valor a process_result.
    # - Para evitar UnboundLocalError e garantir que sempre haja uma resposta,
    #   inicializamos process_result aqui e testamos se ele é None antes de usar.
    push_result = None  # para verificar se o push na fila foi realizado com sucesso e caso tenha erro no processamento, remover o push da fila
    process_result = None  # Inicializar process_result para evitar UnboundLocalError em caminhos onde ele não é atribuído
    result = None  # Inicializar result para garantir que sempre será definido
    status_code = 500  # Inicializar status_code com valor padrão (erro interno)

    # Processamento para método noReplacement
    if replacement_method == "noReplacement" and type_of_process != "contactConfirm":
        removed_items = [item for item in job_items if item.get("status") == "REMOVED"]
        
        if len(job_items) > 0:
            # Extrair dados do cliente para verificação de SKUs processadas
            recipient = input_data.get("job", {}).get("recipient", {})
            phone = recipient.get("phone_number", "")
            full_name = recipient.get("name", "")
            
            processed_skus = []
            urn = None
            
            if phone:
                # Buscar contato na Weni de forma robusta
                contact_data, urn = get_weni_contact_robust(phone, engine)
                
                if contact_data:
                    # Verificar se é um pedido novo (comparar order_id atual com o último armazenado)
                    order_id = get_order_id_from_weni(contact_data, engine)
                    current_order_id = client_reference or ""
                    
                    if order_id and order_id != current_order_id:
                        # Se é um pedido novo (order_id diferente), limpar SKUs processadas
                        processed_skus = []  # Limpar lista de SKUs para novo pedido
                        # Atualizar order_id no contato
                        _, teste = update_weni_contact(urn, full_name, [], engine, order_id=current_order_id, teste=teste if teste else "")
                    else:
                        # Mesmo pedido ou primeiro pedido, usar SKUs processadas existentes
                        processed_skus = get_processed_skus_from_weni(contact_data, engine)
                        if not order_id and current_order_id:
                            # Primeira vez, atualizar order_id
                            _, teste = update_weni_contact(urn, full_name, processed_skus, engine, order_id=current_order_id, teste=teste if teste else "")
            
            # Verificar SKUs para processar (filtrar apenas as não processadas)
            skus_to_process = [item for item in removed_items if item.get("id") and str(item.get("id")) not in processed_skus]
            
            if not skus_to_process:
                removed_skus = [item.get("id", "") for item in removed_items if item.get("id")]
                result = {
                    "Status": "Success",
                    "type": replacement_method,
                    "error": f"nenhuma SKU para verificar, SKUs já verificadas: {removed_skus}",
                    "remove": True,
                    "teste": teste,
                }
                status_code = 409  # Conflict - SKUs já processadas
            else:
                # Processar apenas SKUs não processadas
                for item in skus_to_process:
                    product_id = str(item.get("id") or item.get("sku") or "").strip()
                    if not product_id:
                        continue
                    
                    ruptura_success, teste_instaleap = send_instaleap_external_data(
                        order_id=client_reference,
                        product_id=product_id,
                        ruptura_message="AUT REMOÇÃO - NO_REPLACEMENT",
                        teste=teste
                    )
                    teste = teste_instaleap
                    if not ruptura_success:
                        teste += f"send_instaleap_external_data: retornou_False para produto {product_id} - "
                    
                    # Registrar SKU como processada
                    if phone and urn:
                        if product_id not in processed_skus:
                            updated_skus = processed_skus + [product_id]
                            teste += f"noReplacement: ANTES_SALVAR_SKU product_id={product_id} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
                            save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=client_reference, teste=teste)
                            teste += f"noReplacement: DEPOIS_SALVAR_SKU save_success={save_success} product_id={product_id} - "
                            
                            # Verificar se o SKU foi realmente salvo fazendo uma leitura
                            if save_success and urn:
                                try:
                                    contact_data_verify, _ = get_weni_contact_robust(phone, engine)
                                    if contact_data_verify:
                                        processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                                        sku_salvo = str(product_id) in processed_skus_verify
                                        teste += f"noReplacement: VERIFICACAO_SALVAMENTO sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                                    else:
                                        teste += f"noReplacement: VERIFICACAO_SALVAMENTO contact_data_verify=None - "
                                except Exception as e:
                                    teste += f"noReplacement: ERRO_VERIFICACAO_SALVAMENTO={str(e)} - "
                            else:
                                teste += f"noReplacement: NAO_VERIFICOU_SALVAMENTO save_success={save_success} urn={urn} - "
                            
                            processed_skus = updated_skus  # Atualizar lista local
                        else:
                            teste += f"noReplacement: SKU_JA_PROCESSADO product_id={product_id} processed_skus={processed_skus} - "
                
                result = {
                    "Status": "Success",
                    "type": replacement_method,
                    "message": "Itens removidos sem substituição.",
                    "teste": teste,
                }
                status_code = 200
        else:
            result = {
                "Status": "Success",
                "type": replacement_method,
                "action": "removed",
                "items_removed": [],
                "message": "Nenhum item removido para processar.",
                "teste": teste,
            }
            status_code = 200

    # Processamento para método replacementBySimilar ou contactConfirm
    elif replacement_method in ["replacementBySimilar", "contactConfirm", "noReplacement"]:

        push_result = None  # Inicializar para evitar erro se não entrar no bloco contactConfirm
        fila_inicial_vazia = True  # Inicializar como True (assume fila vazia por padrão)

        # Processamento para método contactConfirm
        if replacement_method == "contactConfirm" or type_of_process == "contactConfirm":
            teste += "replacement_method == contactConfirm ou type_of_process == contactConfirm - "
            
            # Obter dados do cliente e contato
            recipient_tmp = input_data.get("job", {}).get("recipient", {})
            phone_tmp = recipient_tmp.get("phone_number", "")
            removed_items_tmp = [item for item in input_data.get("job", {}).get("job_items", []) if item.get("status") == "REMOVED"]
            removed_ids_tmp = [str(it.get("id")) for it in removed_items_tmp if it.get("id")]
            # Contar apenas SKUs únicos para evitar problemas com duplicatas
            removed_ids_unique = list(dict.fromkeys(removed_ids_tmp))  # Remove duplicatas mantendo ordem
            removed_count_notification = len(removed_ids_unique)
            
            teste += f"verificacao_multiplos_removed: removed_count_notification={removed_count_notification} removed_ids={removed_ids_tmp} removed_ids_unique={removed_ids_unique} - "
            
            # Verificar se é pedido novo e obter SKUs processadas
            contact_data_tmp = None
            processed_skus_tmp = []
            removed_count_saved_weni = 0
            is_new_order = False
            urn_tmp = None  # Inicializar para evitar erro se não entrar no bloco if phone_tmp
            
            if phone_tmp:
                contact_data_tmp, urn_tmp = get_weni_contact_robust(phone_tmp, engine)
                if contact_data_tmp:
                    # Verificar se é pedido novo
                    order_id_weni = get_order_id_from_weni(contact_data_tmp, engine)
                    current_order_id = client_reference or ""
                    
                    if order_id_weni and order_id_weni != current_order_id:
                        # Pedido novo: resetar contagem
                        is_new_order = True
                        removed_count_saved_weni = 0
                        teste += f"verificacao_multiplos_removed: PEDIDO_NOVO order_id_weni={order_id_weni} current={current_order_id} - "
                        # Atualizar removed_count para 0 e order_id
                        _, teste = contact_removed_count(contact_data_tmp, engine, "update", 0, urn_tmp, teste)
                        _, teste = update_weni_contact(urn_tmp, recipient_tmp.get("name", ""), [], engine, order_id=current_order_id, teste=teste)
                    else:
                        # Mesmo pedido: obter SKUs processadas e removed_count salvo
                        processed_skus_tmp = get_processed_skus_from_weni(contact_data_tmp, engine)
                        removed_count_saved_weni, teste = contact_removed_count(contact_data_tmp, engine, "extract", teste=teste)
                        teste += f"verificacao_multiplos_removed: MESMO_PEDIDO processed_skus={processed_skus_tmp} removed_count_saved={removed_count_saved_weni} - "
            
            # Identificar SKUs novos (não processados)
            # Usar removed_ids_unique para evitar problemas com duplicatas
            new_skus = [rid for rid in removed_ids_unique if rid and rid not in processed_skus_tmp]
            new_skus_count = len(new_skus)
            
            # Se houver duplicatas em removed_ids_tmp, logar para debug
            if len(removed_ids_tmp) != len(removed_ids_unique):
                teste += f"verificacao_multiplos_removed: DUPLICATAS_DETECTADAS removed_ids_tmp_count={len(removed_ids_tmp)} removed_ids_unique_count={len(removed_ids_unique)} - "
            
            teste += f"verificacao_multiplos_removed: new_skus={new_skus} new_skus_count={new_skus_count} - "
            
            # Verificar se há múltiplos SKUs novos (>= 2) e se a quantidade na notificação é maior em 2+ unidades
            json_da_fila = get_fila_from_codeaction(client_reference, engine)
            teste += f"get_fila: resultado={'encontrado' if json_da_fila else 'vazio'} pedido={client_reference} - "
            
            # Salvar estado inicial da fila ANTES de fazer push (para usar na verificação de item promocional)
            fila_inicial_vazia = (json_da_fila is None)
            
            should_split_processing = False
            # Verificar duas condições:
            # 1. Há 2 ou mais SKUs novos na notificação atual
            # 2. A quantidade de REMOVED na notificação é maior em 2+ unidades que a salva na Weni
            if not is_new_order and new_skus_count >= 2:
                if removed_count_notification >= (removed_count_saved_weni + 2):
                    # Caso especial: múltiplos SKUs novos na notificação E quantidade maior em 2+
                    # Processar o primeiro e fazer push dos outros na fila
                    should_split_processing = True
                    teste += f"verificacao_multiplos_removed: MULTIPLOS_REMOVED_DETECTADOS (new_skus={new_skus_count}, notif={removed_count_notification} >= saved+2={removed_count_saved_weni}+2) - "
                else:
                    teste += f"verificacao_multiplos_removed: new_skus={new_skus_count} mas notif={removed_count_notification} < saved+2={removed_count_saved_weni}+2 - NAO_SPLIT - "
            
            if should_split_processing:
                # Quando há múltiplos SKUs novos, processamos o primeiro e fazemos push do JSON original na fila
                # Apenas para os SKUs restantes (a partir do índice 1), pois o primeiro será processado imediatamente
                first_sku_id = new_skus[0]
                other_skus = new_skus[1:]
                teste += f"verificacao_multiplos_removed: MULTIPLOS_REMOVED - processando_primeiro_sku={first_sku_id} outros_skus={other_skus} - "
                
                # Fazer push do JSON original completo na fila UMA VEZ PARA CADA SKU RESTANTE
                # O primeiro SKU será processado imediatamente nesta execução, então não deve ir para a fila.
                # IMPORTANTE:
                #   - other_skus já está sem duplicatas (vem de removed_ids_unique via new_skus),
                #     então cada push representa um SKU único diferente.
                #   - Ao ter um push por SKU restante, garantimos que haverá uma linha na fila
                #     para cada item REMOVED, permitindo que cada execução futura trate
                #     exatamente um SKU novo por vez.
                push_success_count = 0
                push_result_split = None  # Armazenar resultado do push para usar depois
                if other_skus:  # Só fazer push se houver SKUs além do primeiro
                    for sku_restante in other_skus:
                        push_result_others = push_fila_from_codeaction(client_reference, input_data, engine)
                        if push_result_others.get("success"):
                            push_success_count += 1
                            push_result_split = push_result_others
                            teste += f"push_fila: SUCESSO JSON_enviado_para_fila_para_SKU_restante={sku_restante} - "
                        else:
                            teste += f"push_fila: ERRO ao_enviar_JSON_para_fila_para_SKU_restante={sku_restante} mensagem={push_result_others.get('message')} - "
                
                # Preservar push_result para usar depois na verificação de remoção da fila
                # Log de debug para verificar o estado de push_result_split antes da condição
                teste += f"verificacao_multiplos_removed: antes_preservacao push_result_split_existe={push_result_split is not None} - "
                if push_result_split:
                    push_result_split_success = push_result_split.get('success', False)
                    push_result_split_msg = push_result_split.get('message', 'N/A')[:50] if push_result_split.get('message') else 'N/A'
                    teste += f"verificacao_multiplos_removed: push_result_split_success={push_result_split_success} push_result_split_msg={push_result_split_msg} - "
                
                if push_result_split and push_result_split.get("success"):
                    push_result = push_result_split
                    teste += f"verificacao_multiplos_removed: push_result_preservado_do_split success=True push_result_agora={push_result is not None} - "
                else:
                    teste += f"verificacao_multiplos_removed: push_result_NAO_preservado push_result_split_existe={push_result_split is not None} push_result_split_success={push_result_split.get('success') if push_result_split else None} - "
                
                # Atualizar removed_count na Weni
                if contact_data_tmp and urn_tmp:
                    _, teste = contact_removed_count(contact_data_tmp, engine, "update", removed_count_notification, urn_tmp, teste)
                    teste += f"verificacao_multiplos_removed: removed_count_atualizado={removed_count_notification} - "
                
                # Se já houver itens na fila E esta notificação não veio da fila (client_reference_param é None),
                # não devemos iniciar um novo fluxo agora para o primeiro SKU.
                # Deixamos apenas o JSON na fila e retornamos 202 "Na fila, esperando processamento".
                if not fila_inicial_vazia and client_reference_param is None:
                    teste += (
                        "verificacao_multiplos_removed: FILA_JA_ATIVA_apenas_push_sem_novo_fluxo_para_primeiro_sku "
                        f"first_sku_id={first_sku_id} fila_inicial_vazia={fila_inicial_vazia} - "
                    )
                    engine.result.set(
                        {
                            "Status": "Na fila, esperando processamento",
                            "type": replacement_method,
                            "sku_original": first_sku_id,
                            "teste": teste,
                        },
                        status_code=202,
                        content_type="json",
                    )
                    return
                
                # Caso contrário (fila estava vazia ou veio da fila), o processamento normal
                # continuará e processará apenas o primeiro SKU novo, pois
                # process_product_replacement identifica SKUs não processados automaticamente.
                teste += f"verificacao_multiplos_removed: processamento_normal_continuara_com_primeiro_sku={first_sku_id} - "
            
            # Continuar com lógica normal
            sku_original = None
            if new_skus:
                sku_original = new_skus[0]
            
            teste += f"sku_original_em_avaliacao={sku_original} processed_skus={processed_skus_tmp} - "

            # Verificar fila e fazer push (se não foi feito no split)
            # push_result já pode ter sido definido no split (linha 2715), então não sobrescrever aqui
            if not should_split_processing:
                # Verificar se o SKU a ser processado é promocional ANTES de fazer push
                # Se for promocional e a fila estiver vazia, não fazer push (será processado diretamente)
                is_sku_promotional = False
                if sku_original and promotional_items:
                    is_sku_promotional = str(sku_original) in [str(pi) for pi in promotional_items]
                    teste += f"verificacao_pre_push: sku_original={sku_original} is_promotional={is_sku_promotional} - "
                
                # Se a fila estiver vazia, é o primeiro contato com o cliente -> push e start_weni_flow
                # EXCETO se for item promocional (itens promocionais são processados diretamente quando a fila está vazia)
                if json_da_fila is None:
                    if is_sku_promotional:
                        # Item promocional com fila vazia: não fazer push, será processado diretamente
                        start_flow = True
                        teste += f"push_fila: fila_vazia_item_promocional_nao_fazendo_push processamento_direto pedido={client_reference} - "
                    else:
                        # Item normal com fila vazia: fazer push normalmente
                        start_flow = True
                        teste += f"push_fila: fila_vazia_iniciando_push pedido={client_reference} - "
                        push_result = push_fila_from_codeaction(client_reference, input_data, engine)
                        if push_result.get("success"):
                            teste += f"push_fila: SUCESSO pedido={client_reference} - "
                        else:
                            error_msg = push_result.get('message', 'Erro desconhecido')
                            teste += f"push_fila: ERRO pedido={client_reference} mensagem={error_msg} - "
                # Se a fila não estiver vazia e start_flow for False, é um contato subsequente -> apenas PUSH
                # EXCETO se for item promocional (itens promocionais têm lógica especial mais abaixo)
                elif not start_flow:
                    # Verificar se é promocional ANTES de fazer push
                    # Se for promocional, não fazer push aqui - deixar o bloco de item promocional fazer
                    if is_sku_promotional:
                        teste += f"push_fila: item_promocional_com_fila_ativa_nao_fazendo_push_aqui (sera_feito_no_bloco_promocional) pedido={client_reference} - "
                        start_flow = True  # Permitir processamento para chegar no bloco promocional
                    else:
                        teste += f"push_fila: contato_subsequente_iniciando_push pedido={client_reference} - "
                    push_result = push_fila_from_codeaction(client_reference, input_data, engine)
                    if push_result.get("success"):
                        teste += f"push_fila: SUCESSO pedido={client_reference} - "
                        engine.result.set({
                            "Status": "Na fila, esperando processamento",
                            "type": replacement_method,
                            "sku_original": sku_original,
                            "teste": teste,
                        }, status_code=202, content_type="json")  # Accepted - processamento assíncrono iniciado
                        return
                    else:
                        error_msg = push_result.get('message', 'Erro desconhecido')
                        teste += f"push_fila: ERRO pedido={client_reference} mensagem={error_msg} - "
            else:
                # Se should_split_processing, start_flow será True para processar o primeiro SKU
                # O push já foi feito no bloco should_split_processing
                start_flow = True
                teste += f"verificacao_multiplos_removed: start_flow=True para_processar_primeiro_sku (push_ja_realizado) - "
            
            # Se não for split, atualizar removed_count na Weni se necessário
            if not should_split_processing and contact_data_tmp and urn_tmp and removed_count_notification > removed_count_saved_weni:
                _, teste = contact_removed_count(contact_data_tmp, engine, "update", removed_count_notification, urn_tmp, teste)
                teste += f"verificacao_multiplos_removed: removed_count_atualizado={removed_count_notification} - "
            
        teste += f"processamento_substituicao: iniciando_process_product_replacement replacement_method={replacement_method} client_reference={client_reference} - "
        process_result = process_product_replacement(input_data, vtex_order, replacement_method, engine, client_reference=client_reference, promotional_items=promotional_items, teste=teste)
        teste = process_result.get("teste", "")
        teste += f"processamento_substituicao: process_result_obtido remove={process_result.get('remove', True)} type={process_result.get('type')} error={process_result.get('error', 'N/A')} - "

        # Se o processo de substituição resultou em remoção, enviar ruptura para o Instaleap. Removendo por que é um item promocional ou não foram encontrados produtos similares.
        instaleap_success = False  # inicializar para evitar UnboundLocalError
        flow_started = False  # Inicializar flow_started para evitar UnboundLocalError
        if process_result.get("remove", True):
            teste += f"processamento_substituicao: REMOCAO_DETECTADA enviando_ruptura_instaleap - "
            original_sku = str(process_result.get("produto_antigo", {}).get("sku", "") or "").strip()
            error_msg = process_result.get("error", "")
            is_promotional = "PROMOCIONAL" in error_msg

            #################################################################
            # NOVO COMPORTAMENTO: contactConfirm SEM OPÇÕES
            # ---------------------------------------------------------------
            # Quando o método é contactConfirm OU type_of_process == "contactConfirm"
            # (ambos os casos são tratados aqui) e não foram encontradas opções
            # de substituição (SUBST NAO ADIC. - SEM OPCOES), ainda assim devemos
            # disparar um fluxo na Weni para avisar o cliente de que o item foi
            # removido sem substituição, marcando is_journey=True e enviando
            # produtos_escolhidos como lista vazia.
            #
            # IMPORTANTE: Antes de disparar, verificar se há fila ativa. Se houver,
            # fazer push na fila e retornar, não disparar imediatamente.
            #################################################################
            if (replacement_method == "contactConfirm" or type_of_process == "contactConfirm") and not is_promotional:
                try:
                    processar_imediatamente = False
                    # Verificar se veio da fila (se já está sendo processado)
                    if client_reference_param is not None:
                        # Veio da fila: processar imediatamente
                        teste += "processamento_substituicao: contactConfirm_sem_opcoes_veio_da_fila processando_imediatamente - "
                        processar_imediatamente = True
                    else:
                        # Não veio da fila: verificar se há fila ativa ANTES de disparar
                        # Usar o estado inicial da fila (antes do push) salvo anteriormente
                        fila_ativa_antes_do_processamento = not fila_inicial_vazia
                        teste += f"processamento_substituicao: contactConfirm_sem_opcoes_verificando_fila fila_ativa_antes_do_processamento={fila_ativa_antes_do_processamento} fila_inicial_vazia={fila_inicial_vazia} - "
                        
                        if fila_ativa_antes_do_processamento:
                            # Há fila ativa: cliente está em conversa -> fazer push na fila e retornar
                            teste += f"processamento_substituicao: contactConfirm_sem_opcoes_FILA_ATIVA fazendo_push_na_fila - "
                            
                            recipient = input_data.get("job", {}).get("recipient", {})
                            phone = recipient.get("phone_number", "")
                            full_name = recipient.get("name", "")
                            
                            # Salvar lista vazia de produtos_escolhidos na Weni ANTES de fazer push
                            if phone:
                                urn = format_phone_to_urn(phone)
                                save_ok, teste_save = save_produtos_escolhidos_to_weni(
                                    urn=urn,
                                    produtos_escolhidos=[],  # Sem opções de substituição
                                    engine=engine,
                                )
                                teste += teste_save
                                teste += f"processamento_substituicao: contactConfirm_sem_opcoes_produtos_vazios_salvos_na_weni save_ok={save_ok} - "
                            
                            # Fazer push do JSON na fila
                            push_result_sem_opcoes = push_fila_from_codeaction(client_reference, input_data, engine)
                            if push_result_sem_opcoes.get("success"):
                                teste += f"processamento_substituicao: contactConfirm_sem_opcoes_push_SUCESSO - "
                                # Preservar push_result para uso posterior
                                if push_result is None:
                                    push_result = push_result_sem_opcoes
                                
                                engine.result.set({
                                    "Status": "Na fila, esperando processamento",
                                    "type": replacement_method,
                                    "sku_original": original_sku,
                                    "message": "Item sem opções - aguardando processamento na fila",
                                    "teste": teste,
                                }, status_code=202, content_type="json")
                                return
                            else:
                                error_msg_push = push_result_sem_opcoes.get('message', 'Erro desconhecido')
                                teste += f"processamento_substituicao: contactConfirm_sem_opcoes_push_ERRO mensagem={error_msg_push} - "
                                # Se push falhar, continuar com processamento imediato (fallback)
                                processar_imediatamente = True
                        else:
                            # Fila vazia: processar imediatamente
                            teste += "processamento_substituicao: contactConfirm_sem_opcoes_FILA_VAZIA processando_imediatamente - "
                            processar_imediatamente = True
                    
                    # Processar imediatamente apenas se não houver fila ativa ou se veio da fila
                    if processar_imediatamente:
                        recipient = input_data.get("job", {}).get("recipient", {})
                        phone = recipient.get("phone_number", "")
                        full_name = recipient.get("name", "")
                        commerce_id = vtex_order.get("salesChannel", "") if vtex_order else ""
                        produto_antigo = process_result.get("produto_antigo", {})

                        #################################################################
                        # Salvar produto faltante em campo de contato da Weni
                        # ---------------------------------------------------------------
                        # Salva informações do produto faltante (sem opções de substituição)
                        # em campo customizado da Weni para uso posterior na jornada desestruturada
                        #################################################################
                        if phone:
                            try:
                                urn = format_phone_to_urn(phone)
                                
                                # Preparar informações do produto faltante
                                item_faltando_info = {
                                    "sku": str(produto_antigo.get("sku", "")),
                                    "quantity": float(produto_antigo.get("quantity", 0)),
                                    "name": produto_antigo.get("name", ""),
                                    "price": float(produto_antigo.get("price", 0)),
                                    "unit": produto_antigo.get("unit", "un"),
                                    "unit_multiplier": float(produto_antigo.get("unit_multiplier", 1.0)),
                                    "photo_url": produto_antigo.get("photo_url", ""),
                                    "order_id": client_reference,
                                }
                                
                                # Salvar em campo customizado da Weni
                                url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
                                headers = {
                                    "Authorization": "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70",
                                    "Content-Type": "application/json",
                                }
                                fields = {
                                    "item_faltando_jornada": json.dumps(item_faltando_info, ensure_ascii=False)
                                }
                                payload = {"fields": fields}
                                
                                teste += f"jornada_desestruturada: salvando_item_faltando sku={item_faltando_info['sku']} quantity={item_faltando_info['quantity']} - "
                                
                                # Timeout reduzido para evitar timeout do contexto de requisição
                                response = requests.post(url, json=payload, headers=headers, timeout=15)
                                response.raise_for_status()
                                
                                success = 200 <= response.status_code < 300
                                teste += f"jornada_desestruturada: item_faltando_salvo_na_weni success={success} status={response.status_code} - "
                                
                            except requests.exceptions.RequestException as e:
                                teste += f"jornada_desestruturada: erro_ao_salvar_item_faltando request_error={str(e)} - "
                            except Exception as e:
                                teste += f"jornada_desestruturada: erro_ao_salvar_item_faltando unexpected_error={type(e).__name__}:{str(e)} - "
                        else:
                            teste += f"jornada_desestruturada: phone_vazio_nao_pode_salvar_item_faltando - "
                        #################################################################

                    # Disparar fluxo Weni com is_journey=True e is_promotional=False
                    if phone:
                        flow_started, teste = start_weni_flow(
                            phone=phone,
                            produtos_escolhidos=[],
                            produto_antigo=produto_antigo,
                            order_id=client_reference,
                            commerce_id=commerce_id,
                            name=full_name,
                            engine=engine,
                            first_contact=first_contact,
                            is_promotional=False,
                            is_journey=True,
                            teste=teste,
                        )
                        teste += f"processamento_substituicao: contactConfirm_sem_opcoes_start_weni_flow_retornou flow_started={flow_started} - "
                except Exception as e:
                    # Não quebrar o fluxo principal em caso de erro ao disparar o fluxo Weni
                    teste += f"processamento_substituicao: contactConfirm_sem_opcoes_erro_ao_disparar_fluxo_weni={e} - "

            # Se for item promocional, verificar se deve ir para fila ou processar imediatamente
            if is_promotional:
                teste += f"processamento_substituicao: ITEM_PROMOCIONAL detectado - "
                
                # Se veio da fila (client_reference_param definido), processar imediatamente
                # Não verificar fila ativa, pois já está sendo processado da fila
                if client_reference_param is not None:
                    teste += f"processamento_substituicao: item_promocional_veio_da_fila processando_imediatamente - "
                    fila_ativa_antes_do_push = False  # Forçar processamento imediato quando vem da fila
                else:
                    # Verificar se há fila ativa (cliente em conversa) antes de processar item promocional
                    # IMPORTANTE: Usar o estado inicial da fila (antes do push), não fazer nova verificação
                    # Se houver fila ativa ANTES do push, o item promocional deve ir para a fila
                    # Se a fila estava vazia ANTES do push, processar imediatamente (mesmo que tenha feito push nesta execução)
                    fila_ativa_antes_do_push = not fila_inicial_vazia
                    teste += f"processamento_substituicao: verificando_fila_para_item_promocional fila_ativa_antes_do_push={fila_ativa_antes_do_push} fila_inicial_vazia={fila_inicial_vazia} - "
                
                push_result_promocional = None  # Inicializar para evitar UnboundLocalError
                item_promocional_foi_para_fila = False  # Flag para indicar se item promocional foi para fila
                
                if fila_ativa_antes_do_push:
                    # Há fila ativa: cliente está em conversa -> item promocional deve ir para fila
                    teste += f"processamento_substituicao: FILA_ATIVA_DETECTADA item_promocional_ira_para_fila - "
                    recipient = input_data.get("job", {}).get("recipient", {})
                    phone = recipient.get("phone_number", "")
                    full_name = recipient.get("name", "")
                    commerce_id = vtex_order.get("salesChannel", "") if vtex_order else ""
                    produto_antigo = process_result.get("produto_antigo", {})
                    
                    # Salvar produtos_escolhidos vazio na Weni para item promocional
                    if phone:
                        urn = format_phone_to_urn(phone)
                        save_ok, teste_save = save_produtos_escolhidos_to_weni(
                            urn=urn,
                            produtos_escolhidos=[],  # Lista vazia para itens promocionais
                            engine=engine,
                        )
                        teste += teste_save
                        teste += f"processamento_substituicao: produtos_escolhidos_vazios_salvos_na_weni_para_item_promocional save_ok={save_ok} - "
                    
                    # Fazer push do JSON para a fila (não disparar fluxo imediatamente)
                    teste += f"processamento_substituicao: fazendo_push_item_promocional_para_fila - "
                    push_result_promocional = push_fila_from_codeaction(client_reference, input_data, engine)
                    if push_result_promocional.get("success"):
                        teste += f"processamento_substituicao: push_item_promocional_para_fila_SUCESSO - "
                        # NÃO enviar ruptura para Instaleap quando vai para fila
                        # A ruptura será enviada quando o item for processado da fila (para evitar duplicação)
                        teste += f"processamento_substituicao: ruptura_instaleap_sera_enviada_quando_item_for_processado_da_fila - "
                        
                        # Retornar status 202 (Accepted) indicando que foi para fila
                        # O fluxo Weni e a ruptura Instaleap serão enviados quando o item for processado da fila
                        engine.result.set({
                            "Status": "Na fila, esperando processamento",
                            "type": replacement_method,
                            "sku_original": original_sku,
                            "message": "Item promocional removido - aguardando processamento na fila",
                            "teste": teste,
                        }, status_code=202, content_type="json")
                        return
                    else:
                        teste += f"processamento_substituicao: push_item_promocional_para_fila_ERRO mensagem={push_result_promocional.get('message', 'Erro desconhecido')} - "
                        # Se o push falhar, continuar processamento normal (fallback)
                        teste += f"processamento_substituicao: push_falhou_continuando_processamento_normal_item_promocional - "
                
                # Se não havia fila ativa antes do push ou push falhou: processar item promocional imediatamente
                if not fila_ativa_antes_do_push or (push_result_promocional is not None and not push_result_promocional.get("success", False)):
                    teste += f"processamento_substituicao: processando_item_promocional_imediatamente (sem_fila_ou_push_falhou) - "
                    recipient = input_data.get("job", {}).get("recipient", {})
                    phone = recipient.get("phone_number", "")
                    full_name = recipient.get("name", "")
                    commerce_id = vtex_order.get("salesChannel", "") if vtex_order else ""
                    produto_antigo = process_result.get("produto_antigo", {})
                    
                    # Salvar produtos_escolhidos vazio na Weni
                    if phone:
                        urn = format_phone_to_urn(phone)
                        save_ok, teste_save = save_produtos_escolhidos_to_weni(
                            urn=urn,
                            produtos_escolhidos=[],  # Lista vazia para itens promocionais (sem substituição)
                            engine=engine,
                        )
                        teste += teste_save
                    
                    # Iniciar fluxo Weni para avisar sobre item promocional removido
                    teste += f"processamento_substituicao: iniciando_start_weni_flow_item_promocional - "
                    flow_started, teste = start_weni_flow(
                        phone=phone,
                        produtos_escolhidos=[],  # Lista vazia para itens promocionais (sem substituição)
                        produto_antigo=produto_antigo,
                        order_id=client_reference,
                        commerce_id=commerce_id,
                        name=full_name,
                        engine=engine,
                        first_contact=first_contact,
                        is_promotional=is_promotional,
                        is_journey=False,  # Item promocional não é journey
                        teste=teste
                    )
                    teste += f"processamento_substituicao: start_weni_flow_item_promocional_retornou flow_started={flow_started} - "
                    if flow_started:
                        teste += f"processamento_substituicao: fluxo_weni_iniciado_com_sucesso_para_item_promocional - "
                    else:
                        teste += f"processamento_substituicao: erro_ao_iniciar_fluxo_weni_para_item_promocional - "
                    
            
            # Enviar ruptura para Instaleap
            # EXCETO quando for contactConfirm com "SEM OPÇÕES" - nesse caso, apenas o fluxo Weni é enviado
            is_contact_confirm_sem_opcoes = (
                (replacement_method == "contactConfirm" or type_of_process == "contactConfirm")
                and "SEM OPCOES" in error_msg
                and not is_promotional
            )
            
            if not is_contact_confirm_sem_opcoes:
                # Enviar ruptura para Instaleap apenas se não for contactConfirm SEM OPÇÕES
                if original_sku:
                    instaleap_success, teste = send_instaleap_external_data(
                        order_id=client_reference,
                        product_id=original_sku,
                        ruptura_message=error_msg.upper(),
                        teste=teste
                    )
                    if not instaleap_success:
                        teste += f"send_instaleap_external_data retornou False para o produto {original_sku} - "
                else:
                    teste += "send_instaleap_external_data: original_sku está vazio, não foi possível enviar ruptura - "
            else:
                # ContactConfirm SEM OPÇÕES: não enviar para Instaleap, apenas fluxo Weni foi enviado
                teste += f"processamento_substituicao: contactConfirm_SEM_OPCOES_nao_enviando_instaleap (apenas_fluxo_weni) - "
                instaleap_success = False  # Não foi enviado
            
            teste += f"processamento_substituicao: preparando_resultado_remocao original_sku={original_sku} instaleap_success={instaleap_success} flow_started={flow_started} - "
            
            # Marcar SKU como processado quando item promocional for realmente processado
            # (seja imediatamente ou da fila)
            if is_promotional and original_sku:
                recipient = input_data.get("job", {}).get("recipient", {})
                phone = recipient.get("phone_number", "")
                full_name = recipient.get("name", "")
                if phone:
                    urn = format_phone_to_urn(phone)
                    contact_data, _ = get_weni_contact_robust(phone, engine)
                    if contact_data:
                        processed_skus = get_processed_skus_from_weni(contact_data, engine)
                        if original_sku not in processed_skus:
                            updated_skus = processed_skus + [str(original_sku)]
                            teste += f"processamento_substituicao: ANTES_SALVAR_SKU_PROMOCIONAL sku_id={original_sku} processed_skus_antes={processed_skus} updated_skus={updated_skus} urn={urn} - "
                            save_success, teste = update_weni_contact(urn, full_name, updated_skus, engine, order_id=client_reference, teste=teste)
                            teste += f"processamento_substituicao: DEPOIS_SALVAR_SKU_PROMOCIONAL save_success={save_success} sku_id={original_sku} - "
                            
                            # Verificar se o SKU foi realmente salvo fazendo uma leitura
                            if save_success and urn:
                                try:
                                    contact_data_verify, _ = get_weni_contact_robust(phone, engine)
                                    if contact_data_verify:
                                        processed_skus_verify = get_processed_skus_from_weni(contact_data_verify, engine)
                                        sku_salvo = str(original_sku) in processed_skus_verify
                                        teste += f"processamento_substituicao: VERIFICACAO_SALVAMENTO_PROMOCIONAL sku_salvo={sku_salvo} processed_skus_apos_salvar={processed_skus_verify} - "
                                    else:
                                        teste += f"processamento_substituicao: VERIFICACAO_SALVAMENTO_PROMOCIONAL contact_data_verify=None - "
                                except Exception as e:
                                    teste += f"processamento_substituicao: ERRO_VERIFICACAO_SALVAMENTO_PROMOCIONAL={str(e)} - "
                            else:
                                teste += f"processamento_substituicao: NAO_VERIFICOU_SALVAMENTO_PROMOCIONAL save_success={save_success} urn={urn} - "
                            
                            teste += f"processamento_substituicao: sku_promocional_marcado_como_processado_agora={original_sku} - "
                        else:
                            teste += f"processamento_substituicao: sku_promocional_ja_estava_marcado_como_processado={original_sku} - "
            
            # Preparar resultado para remoção
            result = {
                "Status": "remoção",
                "type": replacement_method,
                "action": "removed",
                "message": "Item removido sem substituição",
                "teste": teste,
            }
            if "error" in process_result:
                result["error"] = process_result["error"]
                teste += f"processamento_substituicao: erro_incluido_no_resultado={process_result['error']} - "
            
            # Incluir flow_started no resultado se foi iniciado (para itens promocionais)
            if is_promotional:
                result["flow_started"] = flow_started
                teste += f"processamento_substituicao: flow_started_incluido_no_resultado={flow_started} - "
            
            # Determinar status code baseado no tipo de erro
            if "nenhuma SKU para verificar" in error_msg:
                status_code = 409  # Conflict - SKUs já processadas (conflito de estado)
                teste += f"processamento_substituicao: status_code_definido_409 (skus_ja_processadas) - "
            elif "SEM OPCOES" in error_msg or "VALOR ACIMA" in error_msg or "PROMOCIONAL" in error_msg:
                status_code = 422  # Unprocessable Entity - regras de negócio impedem processamento
                teste += f"processamento_substituicao: status_code_definido_422 (regras_negocio) - "
            else:
                status_code = 400  # Bad Request - erro genérico
                teste += f"processamento_substituicao: status_code_definido_400 (erro_generico) - "

            # Remover da fila quando:
            # - houve um PUSH bem-sucedido nesta execução (contato subsequente), OU
            # - esta execução veio da fila (client_reference_param definido), OU
            # - item promocional foi processado (vindo da fila ou não, mas após processamento)
            #
            # IMPORTANTE:
            # Para o caso de remoção SEM OPÇÕES em contactConfirm (replacement_method == "contactConfirm"
            # ou type_of_process == "contactConfirm"), o gerenciamento da fila passa a ser feito
            # exclusivamente pelo fluxo da Weni. Nesses cenários, NÃO devemos fazer pop aqui,
            # mesmo que client_reference_param esteja preenchido ou tenha havido push.
            #
            # Isso evita que o JSON seja removido da fila antes da Weni concluir a jornada.
            teste += (
                f"processamento_substituicao: verificando_se_deve_remover_da_fila "
                f"push_result_success={push_result.get('success') if push_result else False} "
                f"client_reference_param={client_reference_param} "
                f"is_promotional={is_promotional} "
                f"replacement_method={replacement_method} "
                f"type_of_process={type_of_process} - "
            )

            # Flag para identificar explicitamente o cenário de contactConfirm (journey) de remoção sem opções
            is_contact_confirm_journey = (
                (replacement_method == "contactConfirm" or type_of_process == "contactConfirm")
                and not is_promotional
            )

            # Só remover da fila se NÃO for o caso de contactConfirm/journey sem opções
            if (
                not is_contact_confirm_journey
                and (
                    (push_result and push_result.get("success"))
                    or client_reference_param is not None
                    or (is_promotional and not item_promocional_foi_para_fila)
                )
            ):
                # Para itens promocionais processados (não foi para fila), também fazer pop se veio da fila
                if is_promotional and client_reference_param is not None:
                    teste += f"processamento_substituicao: item_promocional_veio_da_fila fazendo_pop - "
                teste += f"pop_fila: removendo_da_fila_apos_remocao pedido={client_reference} - "
                pop_result = pop_fila_from_codeaction(client_reference, engine)
                if pop_result.get("success"):
                    teste += f"pop_fila: SUCESSO pedido={client_reference} - "
                else:
                    teste += f"pop_fila: ERRO pedido={client_reference} mensagem={pop_result.get('message', 'Erro desconhecido')} - "

        else:
            status_code = 200 if "produtos_escolhidos" in process_result else 400
            flow_started = False  # Inicializar flow_started para evitar UnboundLocalError
            result = None  # Inicializar result para garantir que sempre será definido
            
            # Aplicar regras específicas do método replacementBySimilar
            if status_code == 200 and process_result.get("produtos_escolhidos"):
                if replacement_method == "replacementBySimilar" and type_of_process != "contactConfirm":
                    try:
                        original_info = process_result.get("produto_antigo", {})
                        original_sku = str(original_info.get("sku", ""))
                        chosen = process_result["produtos_escolhidos"][0]
                        chosen_sku = str(chosen.get("sku", ""))
                        # Usar quantity diretamente (em kg, ponto flutuante) - NÃO usar quantity_in_units
                        chosen_qty = float(chosen.get("quantity", 1) or 1)

                        teste += f"original_sku={original_sku} chosen_sku={chosen_sku} chosen_qty={chosen_qty} (kg) - "


                        if process_result.get("talk_to_client") == False:
                            teste += " process_result.get('talk_to_client', False) - "
                            # Regra 1: auto-substituir, sem Weni
                            send_replacement_suggestion_to_zaffari(
                                order_id=client_reference,
                                product_id_original=original_sku,
                                product_id_replacement=chosen_sku,
                                quantity=chosen_qty,
                                engine=engine,
                            )
                            
                            # Para auto-substituição em replacementBySimilar, o item não vai para a fila
                            # Só fazer pop se veio da fila (client_reference_param) e foi para a fila em algum momento anterior
                            # Para replacementBySimilar puro (sem contactConfirm), push_result sempre será None porque não faz push
                            teste += f"processamento_substituicao: sucesso_auto_substituicao - replacementBySimilar_nao_vai_para_fila push_result={push_result is not None} client_reference_param={client_reference_param} - "
                            # Apenas fazer pop se veio da fila (foi processado anteriormente e estava aguardando)
                            if client_reference_param is not None:
                                teste += f"pop_fila: removendo_da_fila_apos_auto_substituicao pedido={client_reference} - "
                                pop_result = pop_fila_from_codeaction(client_reference, engine)
                                if pop_result.get("success"):
                                    teste += f"pop_fila: SUCESSO pedido={client_reference} - "
                                else:
                                    teste += f"pop_fila: ERRO pedido={client_reference} mensagem={pop_result.get('message', 'Erro desconhecido')} - "
                            
                            engine.result.set({
                                "Status": "Success",
                                "type": replacement_method,
                                "action": "auto_substituted",
                                "teste": teste,
                            }, status_code=200, content_type="json")
                            return
                        elif process_result.get("talk_to_client") == True:
                            teste += "process_result.get('talk_to_client', True) - "
                            # Regra 2: até +20% -> Enviar mensagem para cliente para confirmação
                            recipient = input_data.get("job", {}).get("recipient", {})
                            phone = recipient.get("phone_number", "")
                            full_name = recipient.get("name", "")
                            commerce_id = vtex_order.get("salesChannel", "") if vtex_order else ""

                            # Sempre atualizar campos de contato (items_partN) antes de disparar fluxo na Weni
                            if phone:
                                urn = format_phone_to_urn(phone)
                                save_ok, teste_save = save_produtos_escolhidos_to_weni(
                                    urn=urn,
                                    produtos_escolhidos=process_result["produtos_escolhidos"],
                                    engine=engine,
                                )
                                teste += teste_save

                            teste += f"replacementBySimilar_talk_to_client_True: chamando_start_weni_flow - "
                            flow_started, teste = start_weni_flow(
                                phone=phone,
                                produtos_escolhidos=process_result["produtos_escolhidos"],
                                produto_antigo=process_result.get("produto_antigo", {}),
                                order_id=client_reference,
                                commerce_id=commerce_id,
                                name=full_name,
                                engine=engine,
                                first_contact=first_contact,
                                is_promotional=False,  # Não é promocional neste caso
                                is_journey=False,  # replacementBySimilar não é journey
                                teste=teste
                            )
                            teste += f"replacementBySimilar_talk_to_client_True: start_weni_flow_retornou flow_started={flow_started} - "
                        elif process_result.get("remove") == True:
                            teste += "process_result.get('remove', True) - "
                            # Regra 3: acima de +20% -> informar ruptura (AUT REMOÇÃO), sem Weni
                            if original_sku:
                                instaleap_success, teste = send_instaleap_external_data(
                                    order_id=client_reference,
                                    product_id=original_sku,
                                    ruptura_message="SUBST NAO ADIC. - VALOR ACIMA",
                                    teste=teste
                                )
                                if not instaleap_success:
                                    teste += f"send_instaleap_external_data retornou False para o produto {original_sku} - "
                            else:
                                teste += f"send_instaleap_external_data: original_sku está vazio, não foi possível enviar ruptura - "
                            
                            # Remover da fila após processamento (mesmo sendo ruptura, o item foi processado)
                            teste += f"processamento_substituicao: ruptura_verificando_remocao_fila push_result_success={push_result.get('success') if push_result else False} client_reference_param={client_reference_param} - "
                            if (push_result and push_result.get("success")) or client_reference_param is not None:
                                teste += f"pop_fila: removendo_da_fila_apos_ruptura pedido={client_reference} - "
                                pop_result = pop_fila_from_codeaction(client_reference, engine)
                                if pop_result.get("success"):
                                    teste += f"pop_fila: SUCESSO pedido={client_reference} - "
                                else:
                                    teste += f"pop_fila: ERRO pedido={client_reference} mensagem={pop_result.get('message', 'Erro desconhecido')} - "
                            
                            engine.result.set({
                                "Status": "Success",
                                "type": replacement_method,
                                "action": "ruptura",
                                "productId": original_sku,
                                "message": "AUT REMOÇÃO",
                            }, status_code=200, content_type="json")
                            return
                        else:
                            # talk_to_client não definido ou valor inesperado
                            teste += f"processamento_substituicao: talk_to_client_nao_definido={process_result.get('talk_to_client')} - "
                    except Exception:
                        pass

                
                elif replacement_method == "contactConfirm" or type_of_process == "contactConfirm":
                    teste += "contactConfirm e type_of_process == contactConfirm - "
                    teste += f"contactConfirm: verificando_start_flow start_flow={start_flow} - "
                    
                    # Se start_flow for True disparar fluxo
                    if start_flow:
                        teste += f"contactConfirm: start_flow=True iniciando_processo_fluxo - "
                        recipient = input_data.get("job", {}).get("recipient", {})
                        phone = recipient.get("phone_number", "")
                        full_name = recipient.get("name", "")   
                        commerce_id = vtex_order.get("salesChannel", "") if vtex_order else ""
                        
                        teste += f"contactConfirm: dados_cliente phone={bool(phone)} full_name={bool(full_name)} commerce_id={commerce_id} - "
                        
                        # Sempre atualizar campos de contato (items_partN) antes de disparar fluxo na Weni
                        if phone:
                            urn = format_phone_to_urn(phone)
                            teste += f"contactConfirm: salvando_produtos_escolhidos_na_weni - "
                            save_ok, teste_save = save_produtos_escolhidos_to_weni(
                                urn=urn,
                                produtos_escolhidos=process_result["produtos_escolhidos"],
                                engine=engine,
                            )
                            teste += teste_save
                            teste += f"contactConfirm: produtos_escolhidos_salvos save_ok={save_ok} - "
                        else:
                            teste += f"contactConfirm: AVISO phone_vazio_nao_pode_salvar_produtos - "
                        
                        # Executar start_weni_flow
                        # is_journey=True apenas quando é "SEM OPÇÕES" (remove=True)
                        # Quando há produtos_escolhidos (remove=False), is_journey=False
                        produtos_escolhidos_list = process_result.get("produtos_escolhidos", [])
                        is_sem_opcoes = len(produtos_escolhidos_list) == 0
                        is_journey_value = is_sem_opcoes  # True apenas quando SEM OPÇÕES
                        
                        teste += f"contactConfirm: chamando_start_weni_flow is_sem_opcoes={is_sem_opcoes} is_journey={is_journey_value} - "
                        flow_started, teste = start_weni_flow(
                            phone=phone,
                            produtos_escolhidos=produtos_escolhidos_list,
                            produto_antigo=process_result.get("produto_antigo", {}),
                            order_id=client_reference,
                            commerce_id=commerce_id,
                            name=full_name,
                            engine=engine,
                            first_contact=first_contact,
                            is_promotional=False,
                            is_journey=is_journey_value,  # True apenas quando SEM OPÇÕES
                            teste=teste
                        )
                        teste += f"contactConfirm: start_weni_flow_retornou flow_started={flow_started} - "
                    else:
                        teste += f"contactConfirm: start_flow=False fluxo_nao_disparado - "
                        flow_started = False
                    
            
            # Preparar resultado com mensagem de erro específica se houver
            teste += f"processamento_substituicao: preparando_resultado_final flow_started={flow_started} status_code={status_code} type={replacement_method} - "
            result = {
                "Status": "Success" if status_code == 200 else "Error",
                "type": replacement_method,
                "flow_started": flow_started,
                "teste": teste,
            }
            
            # Incluir mensagem de erro específica quando houver
            if "error" in process_result:
                result["error"] = process_result["error"]
                # Se for o erro de SKUs já verificadas, adicionar informação adicional
                if "nenhuma SKU para verificar" in process_result["error"]:
                    result["message"] = "Todas as SKUs do pedido já foram processadas anteriormente. Nenhuma substituição será realizada."
            else:
                # Se não entrou no bloco principal, definir result padrão
                if result is None:
                    result = {
                        "Status": "Error",
                        "type": replacement_method,
                        "error": process_result.get("error", "Erro desconhecido no processamento"),
                        "flow_started": flow_started,
                        "teste": teste,
                    }
            
    else:
        # Caso replacement_method não seja nenhum dos valores esperados
        result = {
            "Status": "Error",
            "type": replacement_method or "unknown",
            "error": f"Método de substituição inválido ou não configurado: {replacement_method}",
            "teste": teste,
        }
        status_code = 400
    
    # Garantir que result sempre está definido antes de usar
    if result is None:
        result = {
            "Status": "Error",
            "type": replacement_method or "unknown",
            "error": "Erro inesperado: result não foi definido",
            "teste": teste,
        }
        status_code = 500
    
    # Remover da fila quando processou com sucesso (remove=False, status_code=200)
    # EXCETO para contactConfirm: o fluxo Weni faz o pop automaticamente no final.
    # Só avaliamos process_result se ele tiver sido definido (não for None).
    if process_result is not None and not process_result.get("remove", True) and status_code == 200:
        # Verificar se é contactConfirm - neste caso, o fluxo Weni fará o pop
        is_contact_confirm_method = (replacement_method == "contactConfirm" or type_of_process == "contactConfirm")
        
        if is_contact_confirm_method:
            # Para contactConfirm, o fluxo Weni faz o pop automaticamente no final
            teste += f"processamento_substituicao: sucesso_contactConfirm_nao_fazendo_pop (fluxo_weni_fara_pop) - "
        else:
            # Para outros métodos (replacementBySimilar, etc), fazer pop aqui
            teste += f"processamento_substituicao: sucesso_detectado_verificando_remocao_fila push_result_success={push_result.get('success') if push_result else False} client_reference_param={client_reference_param} - "
            if (push_result and push_result.get("success")) or client_reference_param is not None:
                teste += f"pop_fila: removendo_da_fila_apos_sucesso pedido={client_reference} - "
                pop_result = pop_fila_from_codeaction(client_reference, engine)
                if pop_result.get("success"):
                    teste += f"pop_fila: SUCESSO pedido={client_reference} - "
                else:
                    teste += f"pop_fila: ERRO pedido={client_reference} mensagem={pop_result.get('message', 'Erro desconhecido')} - "
    
    teste += f"processamento_substituicao: retornando_resultado_final Status={result.get('Status')} type={result.get('type')} action={result.get('action', 'N/A')} status_code={status_code} - "
    result["teste"] = teste  # Garantir que teste está sempre no resultado
    engine.result.set(result, status_code=status_code, content_type="json")
    return