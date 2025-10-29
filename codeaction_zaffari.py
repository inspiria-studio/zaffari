import requests
import time
import json
import sys
import re
import itertools


def get_product_suggestions_from_sheet(product_names_list, engine, original_sku_id: str | None = None):
    """
    NOVA FUNÇÃO: Substitui get_product_suggestions_from_gpt
    Busca sugestões diretamente na planilha do Google Sheets via Apps Script

    Args:
        product_names_list (list): Lista de nomes dos produtos originais
        engine: Engine object para logging

    Returns:
        dict: Dicionário mapeando produto original -> lista de 4 sugestões
    """
    try:
        # URL do seu Web App do Google Apps Script
        web_app_url = "https://script.google.com/macros/s/AKfycbw8fFx8qRi5L9PJcpZDV5p3N82knEuTSTRglpTRSNU125Q02459fAKYlrwvrr-imvV2/exec"
        
        payload = {
            "product_names_list": product_names_list
        }
        # Incluir lista de SKUs originais, quando fornecida (string CSV)
        if original_sku_id:
            payload["original_sku_id"] = original_sku_id
        
        engine.log.debug(f"🌐 Enviando requisição para Google Sheets: {product_names_list}")
        
        # Headers para garantir que o JSON seja enviado corretamente
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Python-Requests/2.28.0'
        }
        
        response = requests.post(web_app_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Verificar se a resposta é válida
        if response.status_code != 200:
            engine.log.debug(f"⚠️ Status HTTP inesperado: {response.status_code}")
            return {name: [name] for name in product_names_list}
        
        suggestions = response.json()
        
        # Validar se o retorno tem o formato esperado
        if not isinstance(suggestions, dict):
            engine.log.debug(f"⚠️ Formato de resposta inesperado: {type(suggestions)}")
            return {name: [name] for name in product_names_list}
        
        engine.log.debug(f"✅ Sugestões obtidas da planilha: {suggestions}")
        
        return suggestions
        
    except requests.exceptions.Timeout:
        engine.log.debug("⏰ Timeout na requisição para Google Sheets")
        return {name: [name] for name in product_names_list}
        
    except requests.exceptions.RequestException as e:
        engine.log.debug(f"🌐 Erro na requisição para Google Sheets: {e}")
        return {name: [name] for name in product_names_list}
        
    except json.JSONDecodeError as e:
        engine.log.debug(f"❌ Erro ao decodificar JSON da resposta: {e}")
        return {name: [name] for name in product_names_list}
        
    except Exception as e:
        engine.log.debug(f"❌ Erro geral ao buscar sugestões na planilha: {e}")
        return {name: [name] for name in product_names_list}


def intelligent_search(product_name, url, engine):
    """
    Searches for products by name and collects detailed information, determining seller ID if necessary.

    Args:
        product_name (str): Name of the product to search for
        url (str): Base URL for the search
        engine: Engine object para logging

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

        engine.log.debug(f"API retornou {len(products)} produtos para '{product_name}'")

        for product in products:
            if not product.get("items"):
                engine.log.debug(f"Skipping product {product.get('productId')} due to missing items.")
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

    except requests.exceptions.RequestException as e:
        engine.log.debug(f"Error fetching data for {product_name}: {e}")
    except json.JSONDecodeError as e:
        engine.log.debug(f"Error decoding JSON response for {product_name}: {e}")

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


def select_products(cart_simulation, products_details, engine):
    """
    Selects products based on availability and price comparison.

    Args:
        cart_simulation (dict): Cart simulation result
        products_details (list): List of product details
        engine: Engine object para logging

    Returns:
        list: List containing details of selected products only
    """
    selected_products_details = []

    # Criar mapeamento de item simulation por ID
    simulation_items = {}
    for item in cart_simulation.get("items", []):
        if item.get("availability", "").lower() == "available":
            simulation_items[item.get("id")] = item

    for product_detail in products_details:
        sku_id = product_detail.get("sku_id")
        original_price = product_detail.get("original_price", 0)

        # Verificar se o produto está disponível na simulação
        if sku_id in simulation_items:
            simulation_item = simulation_items[sku_id]
            simulation_price = simulation_item.get("price", 0) / 100

            # Verificar se o preço da simulação é menor ou igual ao preço original
            if simulation_price <= original_price:
                engine.log.debug(
                    f"Product '{product_detail.get('sku_name')}' approved - Original: {original_price}, Simulation: {simulation_price}"
                )
                selected_products_details.append(product_detail)
            else:
                engine.log.debug(
                    f"Product '{product_detail.get('sku_name')}' rejected - price too high - Original: {original_price}, Simulation: {simulation_price}"
                )
        else:
            engine.log.debug(f"Product '{product_detail.get('sku_name')}' not available in simulation")

    return selected_products_details


def cart_simulation(base_url, products_details, seller, quantity, postal_code, country, engine):
    """
    Performs cart simulation to check availability and delivery channel.

    Args:
        base_url (str): Base URL of the API
        products_details (list): List of product details
        seller (str): Seller ID
        quantity (int): Quantity of products
        postal_code (str): Delivery postal code
        country (str): Delivery country
        engine: Engine object para logging

    Returns:
        list: List of details of selected products
    """
    if not products_details:
        engine.log.debug("cart_simulation: Nenhum produto para simular")
        return []
    items = [{"id": product.get("sku_id"), "quantity": quantity, "seller": seller} for product in products_details]
    url = f"{base_url}/api/checkout/pub/orderForms/simulation"
    payload = {"items": items, "country": country}
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        response_data = response.json()
        selected_products = []
        simulation_items = {
            item.get("id"): item
            for item in response_data.get("items", [])
            if item.get("availability", "").lower() == "available"
        }
        for product_detail in products_details:
            sku_id = product_detail.get("sku_id")
            original_price = product_detail.get("original_price", 0)
            if sku_id in simulation_items:
                simulation_price = simulation_items[sku_id].get("price", 0) / 100
                if simulation_price <= original_price:
                    selected_products.append(product_detail)
        return selected_products
    except requests.exceptions.RequestException as e:
        # Printar o conteúdo da resposta se for erro HTTP
        if hasattr(e, "response") and e.response is not None:
            engine.log.debug(f"cart_simulation: HTTP error {e.response.status_code} - {e.response.text}")
        engine.log.debug(f"Error in cart simulation: {e}")
        return []


def get_google_docs_content(document_id):
    """
    Função simplificada - retorna None pois não usa mais Google Docs
    """
    return None


def get_product_name_suggestions_from_sheet(original_product_name, engine, original_sku_id: str | None = None):
    """
    NOVA FUNÇÃO: Substitui get_product_name_suggestions_from_gpt
    Busca sugestões de nomes alternativos na planilha
    """
    try:
        # Usar a função principal com lista de um produto
        suggestions_dict = get_product_suggestions_from_sheet([original_product_name], engine, original_sku_id)
        
        # Retornar as sugestões do produto
        return suggestions_dict.get(original_product_name, [original_product_name])
        
    except Exception as e:
        engine.log.debug(f"Erro ao gerar sugestões de nomes na planilha: {e}")
        return [original_product_name] * 4


def get_products_from_gpt(original_product, available_products, max_products, engine):
    """
    Segunda chamada GPT: escolhe produtos da lista de produtos encontrados
    NOVA FUNCIONALIDADE: Agora também decide quantidade e regras de preço
    """
    try:
        engine.log.debug("🔍 SELEÇÃO DE PRODUTOS:")
        engine.log.debug(f"   Produto original: {original_product['name']} (R$ {original_product['price']:.2f})")
        engine.log.debug(f"   Máximo de produtos: {max_products}")

        if not available_products:
            engine.log.debug("   ❌ Nenhum produto disponível encontrado")
            return []

        products_list = []
        product_mapping = {}

        idx = 0
        for product_name, product_data in available_products.items():
            for variation in product_data.get("variations", []):
                products_list.append(
                    f"{idx}. {variation.get('sku_name', '')} - Marca: {product_data.get('brand', '')} - Preço: R$ {variation.get('price', 0):.2f}"
                )
                product_mapping[idx] = {
                    "sku_id": variation.get("sku_id", ""),
                    "name": variation.get("sku_name", ""),
                    "price": variation.get("price", 0),
                }
                idx += 1

        engine.log.debug(f"   📦 {len(products_list)} produtos disponíveis para escolha:")
        for i, product_line in enumerate(products_list[:5]):  # Mostra apenas os primeiros 5
            engine.log.debug(f"     {product_line}")
        if len(products_list) > 5:
            engine.log.debug(f"     ... e mais {len(products_list) - 5} produtos")

            # continue logging (no functional change)

        if not products_list:
            engine.log.debug("   ❌ Lista de produtos vazia")
            return []

        products_text = "\n".join(products_list)

        # Buscar prompt do Google Docs para seleção de produtos
        google_docs_id = "1ZiPI0o2HzthkEtc4XrqAiyQYD-IaRxGxBQtlzCdjgOs"
        google_docs_prompt = get_google_docs_content(google_docs_id)

        # Prompt padrão base para seleção de produtos COM DECISÃO DE QUANTIDADE E PREÇO
        original_quantity = original_product.get("quantity", 1)
        base_prompt = f"""
        PRODUTO ORIGINAL REMOVIDO: "{original_product['name']}" - R$ {original_product['price']:.2f} - Quantidade: {original_quantity}

        PRODUTOS SIMILARES DISPONÍVEIS:
        {products_text}

        TAREFA: Escolha até {max_products} MELHORES produtos como alternativas e DEFINA as quantidades específicas.

        ⚠️ REGRA FUNDAMENTAL DE QUANTIDADE:
        - NUNCA sugerir quantidade SUPERIOR à original ({original_quantity})
        - Quantidade máxima permitida: {original_quantity} unidades
        - Você pode sugerir quantidade IGUAL ou MENOR

        ESTRATÉGIAS PERMITIDAS:

        1. QUANTIDADE IGUAL ({original_quantity} unidades):
           - Produto com preço similar ao original
           - Produto mais barato (cliente economiza)
           - Produto mais caro (cliente paga diferença)

        2. QUANTIDADE MENOR (1 até {original_quantity-1 if original_quantity > 1 else 1} unidades):
           - Produto premium mais caro
           - Produto de melhor qualidade
           - Cliente paga menos no total

        REGRAS DE PREÇO:
        - Produto mais barato: pode manter quantidade {original_quantity} (cliente economiza)
        - Produto mais caro: reduzir quantidade para não exceder muito o valor original
        - Sempre priorizar valor total próximo ao original quando possível

        EXEMPLOS PRÁTICOS:
        - Original: R$ 30,00 x 2 unidades = R$ 60,00 total
        - Alternativa 1: R$ 25,00 x 2 unidades = R$ 50,00 (economia de R$ 10,00)
        - Alternativa 2: R$ 45,00 x 1 unidade = R$ 45,00 (economia de R$ 15,00)
        - Alternativa 3: R$ 35,00 x 2 unidades = R$ 70,00 (acréscimo de R$ 10,00)

        FORMATO DE RESPOSTA:
        Retorne APENAS um JSON com esta estrutura:
        {{
            "produtos_selecionados": [
                {{
                    "indice": 0,
                    "quantidade_sugerida": 1,
                    "justificativa": "Produto premium mais caro, 1 unidade para não exceder muito o valor original"
                }},
                {{
                    "indice": 3,
                    "quantidade_sugerida": {original_quantity},
                    "justificativa": "Produto similar mais barato, mantendo quantidade original para economia"
                }}
            ],
            "estrategia_preco": "Priorizar economia" ou "Manter valor similar" ou "Aceitar pequeno acréscimo por qualidade"
        }}

        LEMBRE-SE: Quantidade máxima = {original_quantity}. NUNCA exceda este limite!
        """

        # Concatenar prompt base com complemento do Google Docs
        if google_docs_prompt:
            # Substituir placeholders no complemento do Google Docs
            google_docs_complement = google_docs_prompt.replace("{original_product_name}", original_product["name"])
            google_docs_complement = google_docs_complement.replace("{products_text}", products_text)
            google_docs_complement = google_docs_complement.replace("{max_products}", str(max_products))
            # Adicionar novos placeholders
            google_docs_complement = google_docs_complement.replace(
                "{original_price}", f"R$ {original_product['price']:.2f}"
            )
            google_docs_complement = google_docs_complement.replace("{original_quantity}", str(original_quantity))
            prompt = base_prompt + "\n\n" + google_docs_complement
            engine.log.debug("Usando prompt base + complemento do Google Docs para seleção de produtos")
        else:
            prompt = base_prompt
            engine.log.debug("Usando apenas prompt base para seleção de produtos")

        # NOVA IMPLEMENTAÇÃO: Usar lógica simples em vez de GPT
        # Selecionar produtos baseado em critérios simples
        chosen_products = []
        original_quantity = original_product.get("quantity", 1)
        
        # Ordenar produtos por preço (mais baratos primeiro)
        sorted_products = []
        for product_name, product_data in available_products.items():
            for variation in product_data.get("variations", []):
                sorted_products.append({
                    "sku_id": variation.get("sku_id", ""),
                    "name": variation.get("sku_name", ""),
                    "price": variation.get("price", 0),
                    "product_name": product_name,
                    "product_data": product_data
                })
        
        # Ordenar por preço
        sorted_products.sort(key=lambda x: x["price"])
        
        # Selecionar até max_products
        for i, product in enumerate(sorted_products[:max_products]):
            # Calcular quantidade baseada no preço
            if product["price"] <= original_product["price"]:
                # Produto mais barato - pode manter quantidade original
                qty_sugerida = original_quantity
                justificativa = f"Produto mais barato (R$ {product['price']:.2f} vs R$ {original_product['price']:.2f})"
            else:
                # Produto mais caro - reduzir quantidade
                qty_sugerida = max(1, int(original_quantity * 0.5))  # 50% da quantidade original
                justificativa = f"Produto mais caro, quantidade reduzida para {qty_sugerida}"
            
            # Garantir que não exceda quantidade original
            if qty_sugerida > original_quantity:
                qty_sugerida = original_quantity
                justificativa += f" (limitado a {original_quantity})"
            
            chosen_products.append({
                "sku_id": product["sku_id"],
                "name": product["name"],
                "price": product["price"],
                "quantity": qty_sugerida,
                "justificativa": justificativa
            })
        
        engine.log.debug(f"   ✅ {len(chosen_products)} produtos selecionados com quantidades definidas")
        
        # Log dos produtos selecionados
        for product in chosen_products:
            engine.log.debug(f"     ✅ {product['name']} - R$ {product['price']:.2f} x{product['quantity']}")
            engine.log.debug(f"        💡 {product['justificativa']}")
        
        return chosen_products

    except Exception as e:
        engine.log.debug(f"   ❌ Erro na seleção de produtos: {e}")
        return []


def search_products_with_suggested_names(original_product_name, original_price, base_url, engine, original_sku_id=None):
    """
    Nova função que implementa a arquitetura de busca sequencial:
    1. GPT sugere 4 nomes alternativos
    2. Busca cada nome sequencialmente até encontrar produtos COM ESTOQUE
    3. Retorna os produtos encontrados na primeira busca bem-sucedida
    IMPORTANTE: Filtra o produto original (mesmo SKU) dos resultados
    """
    try:
        # Passo 1: Obter sugestões de nomes do Google Sheets
        suggested_names = get_product_name_suggestions_from_sheet(original_product_name, engine)
        engine.log.debug(f"Nomes sugeridos da planilha para '{original_product_name}': {suggested_names}")

        # Passo 2: Buscar sequencialmente cada nome sugerido
        for i, suggested_name in enumerate(suggested_names):
            engine.log.debug(f"Tentativa {i+1}/4: Buscando por '{suggested_name}'")

            # Fazer busca no intelligent_search
            url = f"{base_url}/api/io/_v/api/intelligent-search/product_search/"
            products_structured = intelligent_search(suggested_name, url, engine)

            if products_structured:
                engine.log.debug(f"Encontrou {len(products_structured)} produtos com '{suggested_name}'")

                # Filtrar produtos com preço <= original E diferentes do produto original
                filtered_products = {}
                for product_name_vtex, product_data in products_structured.items():
                    filtered_variations = []
                    for variation in product_data["variations"]:
                        # Filtrar por preço E excluir o produto original
                        if variation.get("sku_id") == original_sku_id:
                            engine.log.debug(
                                f"Produto original encontrado e filtrado: {variation.get('sku_name')} (SKU: {original_sku_id})"
                            )
                        elif variation.get("price", 0) <= original_price:
                            filtered_variations.append(variation)

                    if filtered_variations:
                        product_data_copy = product_data.copy()
                        product_data_copy["variations"] = filtered_variations
                        filtered_products[product_name_vtex] = product_data_copy

                if filtered_products:
                    # Verificar estoque dos produtos encontrados
                    engine.log.debug("Verificando estoque dos produtos encontrados...")
                    selected_products = []
                    for product_name_vtex, product_data in filtered_products.items():
                        for variation in product_data["variations"]:
                            selected_products.append(
                                {
                                    "sku_id": variation["sku_id"],
                                    "sku_name": variation["sku_name"],
                                    "variations": variation["variations"],
                                    "description": product_data["description"],
                                    "brand": product_data["brand"],
                                    "specification_groups": product_data["specification_groups"],
                                    "original_price": variation.get("price", 0),
                                }
                            )

                    # Simular carrinho
                    products_with_stock = cart_simulation(
                        base_url=base_url,
                        products_details=selected_products,
                        seller="1",
                        quantity=1,
                        postal_code="57063-450",
                        country="BRA",
                        engine=engine,
                    )

                    if products_with_stock:
                        engine.log.debug(
                            f"Produtos com estoque encontrados com '{suggested_name}': {len(products_with_stock)} produtos"
                        )
                        return filtered_products
                    else:
                        engine.log.debug(
                            f"Produtos encontrados mas sem estoque para '{suggested_name}', tentando próxima sugestão..."
                        )
                else:
                    engine.log.debug(f"Nenhum produto com preço adequado encontrado para '{suggested_name}'")
            else:
                engine.log.debug(f"Nenhum produto encontrado para '{suggested_name}'")

        # Se chegou aqui, nenhuma das 4 tentativas encontrou produtos com estoque
        engine.log.debug(f"Nenhum produto com estoque encontrado após 4 tentativas para '{original_product_name}'")
        return {}

    except Exception as e:
        engine.log.debug(f"Erro na busca com nomes sugeridos: {e}")
        return {}


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

    except requests.exceptions.RequestException as e:
        engine.log.debug(f"Error fetching VTEX order details: {e}")
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
        headers = {"Authorization": "407d37d44b276f3c2d33c276bc9ac763d4e3e1c6"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return results[0] if results else None

    except requests.exceptions.RequestException as e:
        engine.log.debug(f"Error fetching Weni contact: {e}")
        return None


def update_weni_contact(urn, name, skus_list, engine):
    """
    Atualiza contato na API da Weni com nova lista de SKUs
    """
    try:
        url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
        headers = {
            "Authorization": "407d37d44b276f3c2d33c276bc9ac763d4e3e1c6",  # TODO
            "Content-Type": "application/json",
        }

        payload = {"name": name, "fields": {"sku": json.dumps(skus_list)}}

        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return True

    except requests.exceptions.RequestException as e:
        engine.log.debug(f"Error updating Weni contact: {e}")
        return False


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
            engine.log.debug(f"Erro ao processar campo SKU da Weni: {e}")
    return []


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


def process_product_replacement(input_data, vtex_order, replacement_method, engine):
    """
    Função unificada para processar replacementBySimilar e contactConfirm
    """
    # Extrair dados do cliente do webhook (recipient) - SEMPRE do job
    recipient = input_data.get("job", {}).get("recipient", {})
    phone = recipient.get("phone_number", "")
    full_name = recipient.get("name", "")
    if not phone:
        return {
            "status": replacement_method,
            "error": "Telefone não encontrado no pedido",
        }
    # Buscar contato na Weni de forma robusta
    contact_data, urn = get_weni_contact_robust(phone, engine)
    processed_skus = get_processed_skus_from_weni(contact_data, engine) if contact_data else []
    # Obter itens REMOVED
    job_items = input_data.get("job", {}).get("job_items", [])
    removed_items = [item for item in job_items if item.get("status") == "REMOVED"]
    # Verificar regra dos 50%
    if len(job_items) > 0 and (len(removed_items) / len(job_items)) > 0.5:
        return {
            "status": replacement_method,
            "error": "acima de 50% de produtos com o status REMOVED",
        }
    # Verificar SKUs para processar
    skus_to_process = [item for item in removed_items if item.get("id") and item.get("id") not in processed_skus]
    if not skus_to_process:
        removed_skus = [item.get("id", "") for item in removed_items if item.get("id")]
        return {
            "status": replacement_method,
            "error": f"nenhuma SKU para verificar, SKUs já verificadas: {removed_skus}",
        }
    # Processar primeiro SKU
    item_to_process = skus_to_process[0]
    product_name = item_to_process["name"]
    sku_id = item_to_process["id"]
    quantity = item_to_process["quantity"]
    original_price = item_to_process["price"]
    # Buscar produtos similares usando a nova arquitetura
    base_url = "https://hmlzaffari.myvtex.com"

    # Nova arquitetura: Google Sheets sugere 4 nomes, busca sequencial até encontrar produtos
    products_structured = search_products_with_suggested_names(
        product_name, original_price, base_url, engine, original_sku_id=sku_id
    )
    # Filtrar produtos diferentes do SKU original E com preço <= original
    selected_products = []
    for product_name_vtex, product_data in products_structured.items():
        for variation in product_data["variations"]:
            if variation.get("sku_id") != sku_id and variation.get("price", 0) <= original_price:
                selected_products.append(
                    {
                        "sku_id": variation["sku_id"],
                        "sku_name": variation["sku_name"],
                        "variations": variation["variations"],
                        "description": product_data["description"],
                        "brand": product_data["brand"],
                        "specification_groups": product_data["specification_groups"],
                        "original_price": variation.get("price", 0),
                    }
                )
    
    # Se não encontrou produtos com preço <= original, buscar produtos até 20% mais caros
    if not selected_products:
        max_price = original_price * 1.20  # 20% a mais que o preço original
        for product_name_vtex, product_data in products_structured.items():
            for variation in product_data["variations"]:
                if variation.get("sku_id") != sku_id and variation.get("price", 0) <= max_price:
                    selected_products.append(
                        {
                            "sku_id": variation["sku_id"],
                            "sku_name": variation["sku_name"],
                            "variations": variation["variations"],
                            "description": product_data["description"],
                            "brand": product_data["brand"],
                            "specification_groups": product_data["specification_groups"],
                            "original_price": variation.get("price", 0),
                        }
                    )
    
    if not selected_products:
        # Registrar SKU como processada mesmo sem encontrar produtos
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            update_weni_contact(urn, full_name, updated_skus, engine)
        return {
            "status": replacement_method,
            "error": f"nenhum produto foi encontrado com preço até 20% maior que o original para o SKU {sku_id}",
        }
    # Simular carrinho
    products_with_stock = cart_simulation(
        base_url=base_url,
        products_details=selected_products,
        seller="1",
        quantity=quantity,
        postal_code="57063-450",
        country="BRA",
        engine=engine,
    )
    if not products_with_stock:
        # Registrar SKU como processada
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            update_weni_contact(urn, full_name, updated_skus, engine)
        return {
            "status": replacement_method,
            "error": f"produtos encontrados mas nenhum passou na simulação de estoque (SKU: {sku_id})",
        }
    # Filtrar produtos estruturados com estoque
    sku_ids_with_stock = {product.get("sku_id") for product in products_with_stock}
    products_structured_with_stock = {}
    for product_name_vtex, product_data in products_structured.items():
        filtered_variations = [
            v for v in product_data["variations"] if v.get("sku_id") in sku_ids_with_stock and v.get("price", 0) <= original_price
        ]
        if filtered_variations:
            product_data_copy = product_data.copy()
            product_data_copy["variations"] = filtered_variations
            products_structured_with_stock[product_name_vtex] = product_data_copy
    # Escolher produtos com GPT - até 3 para contactConfirm, 1 para replacementBySimilar
    max_products = 3 if replacement_method == "contactConfirm" else 1
    original_product_data = {"name": product_name, "sku_id": sku_id, "price": original_price}
    chosen_products = get_products_from_gpt(
        original_product=original_product_data, available_products=products_structured_with_stock, max_products=max_products, engine=engine
    )
    if not chosen_products:
        # Registrar SKU como processada
        if sku_id not in processed_skus:
            updated_skus = processed_skus + [sku_id]
            update_weni_contact(urn, full_name, updated_skus, engine)
        return {
            "status": replacement_method,
            "error": f"nenhum produto foi encontrado com preço igual ou inferior ao original para o SKU {sku_id}",
        }
    # Atualizar Weni com SKU processada (só adicionar se não existir)
    if sku_id not in processed_skus:
        updated_skus = processed_skus + [sku_id]
        update_weni_contact(urn, full_name, updated_skus, engine)
    # Preparar resultado: usar quantidades definidas pelo GPT ou calcular automaticamente
    produtos_escolhidos = []
    valor_alvo = original_price * quantity

    for produto in chosen_products:
        if produto["price"] > 0:
            # Usar quantidade definida pelo GPT se disponível
            if "quantity" in produto and produto["quantity"] > 0:
                qty_sugerida = produto["quantity"]
                engine.log.debug(f"   📦 Usando quantidade do GPT: {qty_sugerida} para {produto['name']}")
            else:
                # Fallback: calcular quantidade baseada no valor alvo
                qty_sugerida = max(1, int(valor_alvo // produto["price"]))
                engine.log.debug(f"   📦 Calculando quantidade automaticamente: {qty_sugerida} para {produto['name']}")

            # VALIDAÇÃO FINAL: Garantir que quantidade nunca exceda a original
            if qty_sugerida > quantity:
                engine.log.debug(f"   ⚠️ VALIDAÇÃO FINAL: Quantidade {qty_sugerida} reduzida para {quantity} (máximo permitido)")
                qty_final = quantity
                justificativa_extra = f" (quantidade limitada ao máximo de {quantity})"
            else:
                qty_final = qty_sugerida
                justificativa_extra = ""

            produtos_escolhidos.append(
                {"sku": produto["sku_id"], "name": produto["name"], "price": produto["price"], "quantity": qty_final}
            )

        if len(produtos_escolhidos) == 3:
            break
    return {
        "status": replacement_method,
        "produtos_escolhidos": produtos_escolhidos,
        "produto_antigo": {"sku": sku_id, "name": product_name, "price": original_price, "quantity": quantity},
    }


def Run(engine):
    body = engine.body
    body_dict = json.loads(body)
    input_data = body_dict

    # Obter detalhes do pedido VTEX
    client_reference = input_data.get("job", {}).get("client_reference", "")
    if not client_reference:
        engine.result.set({"error": "client_reference não encontrado"}, status_code=400, content_type="json")
        return

    vtex_order = get_vtex_order_details(client_reference, engine)
    if not vtex_order:
        engine.result.set({"error": "Pedido não encontrado na VTEX"}, status_code=400, content_type="json")
        return

    # Verificar replacementMethod
    custom_apps = vtex_order.get("customData", {}).get("customApps", [])
    replacement_method = None

    for app in custom_apps:
        if app.get("id") == "order":
            replacement_method = app.get("fields", {}).get("replacementMethod")
            break

    # Processar baseado no método
    if replacement_method == "noReplacement":
        job_items = input_data.get("job", {}).get("job_items", [])

        # Verificar se tem PENDING
        if any(item.get("status") == "PENDING" for item in job_items):
            result = {"type": "noReplacement", "error": "Ainda tem pedido como PENDING"}
        # Verificar regra dos 50%
        elif len(job_items) > 0:
            removed_count = sum(1 for item in job_items if item.get("status") == "REMOVED")
            if (removed_count / len(job_items)) > 0.5:
                result = {"type": "noReplacement", "error": "acima de 50% de produtos com o status REMOVED"}
            else:
                result = []
        else:
            result = []

        engine.result.set(result, status_code=200, content_type="json")

    elif replacement_method in ["replacementBySimilar", "contactConfirm"]:
        result = process_product_replacement(input_data, vtex_order, replacement_method, engine)
        status_code = 200 if "produtos_escolhidos" in result else 400
        engine.result.set(result, status_code=status_code, content_type="json")

    else:
        # Código original para outros casos
        recipient = input_data.get("job", {}).get("recipient", {})
        job_items = input_data.get("job", {}).get("job_items", [])

        skus_ids_param = []
        for item in job_items:
            product_id = item.get("id", "")
            product_name = item.get("name", "")
            quantity = item.get("quantity", 1)
            price = item.get("price", 0)

            if product_id and product_name:
                skus_ids_param.append(
                    {"product_id": product_id, "product_name": product_name, "quantity": quantity, "price": price}
                )

        # Continuar com processamento original...
        base_url = "https://hmlzaffari.myvtex.com"
        url = f"{base_url}/api/io/_v/api/intelligent-search/product_search/"

        product_names_list = [item["product_name"] for item in skus_ids_param]
        # Montar CSV de SKUs originais para exclusão (se existir)
        original_sku_ids_csv = ", ".join([item["product_id"] for item in skus_ids_param if item.get("product_id")])
        product_optimizations = get_product_suggestions_from_sheet(product_names_list, engine, original_sku_ids_csv)

        final_results = {}

        for product_name_sku in skus_ids_param:
            original_name = product_name_sku["product_name"]
            suggestions = product_optimizations.get(original_name, [original_name])

            # Tentar cada sugestão até encontrar produtos
            products_structured = {}
            final_search_term = original_name  # Inicializar com nome original
            engine.log.debug(f"🔍 BUSCANDO SUBSTITUTOS PARA: {original_name}")
            engine.log.debug(f"   Testando {len(suggestions)} sugestões da planilha...")

            for i, suggestion in enumerate(suggestions, 1):
                engine.log.debug(f"   Tentativa {i}/{len(suggestions)}: '{suggestion}'")
                products_structured = intelligent_search(suggestion, url, engine)
                if products_structured:  # Se encontrou produtos, para
                    engine.log.debug(f"   ✅ Encontrou {len(products_structured)} produtos com '{suggestion}'")
                    final_search_term = suggestion
                    break
                else:
                    engine.log.debug(f"   ❌ Nenhum produto encontrado para '{suggestion}'")

            # Se não encontrou com nenhuma sugestão, usar nome original
            if not products_structured:
                engine.log.debug(f"   🔄 Tentando nome original como última opção: '{original_name}'")
                products_structured = intelligent_search(original_name, url, engine)
                final_search_term = original_name
                if products_structured:
                    engine.log.debug(f"   ✅ Encontrou {len(products_structured)} produtos com nome original")
                else:
                    engine.log.debug("   ❌ Nenhum produto encontrado nem com nome original")

            sku_id = product_name_sku["product_id"]
            product_name = product_name_sku["product_name"]
            quantity = product_name_sku["quantity"]
            price = product_name_sku["price"]

            selected_products = []
            for product_name_vtex, product_data in products_structured.items():
                for variation in product_data["variations"]:
                    if variation.get("sku_id") and variation.get("sku_id") != sku_id:
                        selected_products.append(
                            {
                                "sku_id": variation["sku_id"],
                                "sku_name": variation["sku_name"],
                                "variations": variation["variations"],
                                "description": product_data["description"],
                                "brand": product_data["brand"],
                                "specification_groups": product_data["specification_groups"],
                                "original_price": variation.get("price", 0),
                            }
                        )

            products_with_stock = cart_simulation(
                base_url=base_url,
                products_details=selected_products,
                seller="1",
                quantity=quantity,
                postal_code="57063-450",
                country="BRA",
                engine=engine,
            )

            # Filtrar produtos estruturados com estoque
            sku_ids_with_stock = {product.get("sku_id") for product in products_with_stock} if products_with_stock else set()
            products_structured_with_stock = {}

            for product_name_vtex, product_data in products_structured.items():
                filtered_variations = [v for v in product_data["variations"] if v.get("sku_id") in sku_ids_with_stock]
                if filtered_variations:
                    product_data_copy = product_data.copy()
                    product_data_copy["variations"] = filtered_variations
                    products_structured_with_stock[product_name_vtex] = product_data_copy

            original_product_data = {
                "name": product_name,
                "sku_id": sku_id,
                "price": price,
                "quantity": quantity,
            }

            chosen_products = get_products_from_gpt(
                original_product=original_product_data,
                available_products=products_structured_with_stock,
                max_products=3,  # Até 3 produtos também no código original
                engine=engine,
            )

            if chosen_products:
                # Preparar lista de produtos escolhidos com quantidade ajustada
                produtos_escolhidos = []
                for chosen_product in chosen_products:
                    suggested_price = chosen_product["price"]

                    # Usar quantidade definida pelo GPT se disponível
                    if "quantity" in chosen_product and chosen_product["quantity"] > 0:
                        adjusted_qty = chosen_product["quantity"]
                        engine.log.debug(f"   📦 Usando quantidade calculada: {adjusted_qty} para {chosen_product['name']}")
                    else:
                        # Fallback: calcular quantidade baseada no valor alvo
                        if suggested_price <= price:
                            adjusted_qty = quantity
                        else:
                            total_budget = price * quantity
                            adjusted_qty = max(1, min(quantity, int(total_budget / suggested_price)))
                        engine.log.debug(f"   📦 Calculando quantidade automaticamente: {adjusted_qty} para {chosen_product['name']}")

                    produtos_escolhidos.append(
                        {
                            "sku": chosen_product["sku_id"],
                            "name": chosen_product["name"],
                            "price": suggested_price,
                            "quantity": adjusted_qty,
                        }
                    )

                final_results[final_search_term] = {
                    "produtos_escolhidos": produtos_escolhidos,
                    "produto_antigo": {"sku": sku_id, "name": product_name, "price": price, "quantity": quantity},
                }
            else:
                final_results[final_search_term] = {
                    "produtos_escolhidos": [],
                    "produto_antigo": {"sku": sku_id, "name": product_name, "price": price, "quantity": quantity},
                }

        json_data = json.dumps(final_results, indent=2, ensure_ascii=False)
        engine.result.set(json_data, status_code=200, content_type="json")
