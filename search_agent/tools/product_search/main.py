from weni import Tool
from weni.context import Context
from weni.responses import TextResponse
import requests
import json
from typing import Tuple, Optional, Dict, Any


class ProductSearch(Tool):
    def execute(self, context: Context) -> TextResponse:
        product_name = context.parameters.get("product_name")
        if product_name is None or (isinstance(product_name, str) and not product_name.strip()):
            return TextResponse(data=json.dumps({"error": "Parâmetro 'product_name' é obrigatório e não pode estar vazio."}, ensure_ascii=False))
        product_name = product_name.strip() if isinstance(product_name, str) else str(product_name)
        print(f"Buscando produtos para: {product_name}")

        # Extrair parâmetros do produto faltante (opcionais)
        original_sku = None
        param_sku = context.parameters.get("sku")
        if param_sku is not None and param_sku != "":
            original_sku = str(param_sku)

        original_price = None
        param_price = context.parameters.get("price")
        if param_price is not None:
            if not isinstance(param_price, (int, float)) or isinstance(param_price, bool):
                return TextResponse(data=json.dumps({"error": "Parâmetro 'price' inválido. Deve ser um número."}, ensure_ascii=False))
            original_price = float(param_price)
            if original_price < 0:
                return TextResponse(data=json.dumps({"error": "Parâmetro 'price' deve ser um número positivo."}, ensure_ascii=False))

        original_quantity = None
        param_quantity = context.parameters.get("quantity")
        if param_quantity is not None:
            if not isinstance(param_quantity, (int, float)) or isinstance(param_quantity, bool):
                return TextResponse(data=json.dumps({"error": "Parâmetro 'quantity' inválido. Deve ser um número."}, ensure_ascii=False))
            original_quantity = float(param_quantity)
            if original_quantity < 0:
                return TextResponse(data=json.dumps({"error": "Parâmetro 'quantity' deve ser um número positivo."}, ensure_ascii=False))

        original_unit = context.parameters.get("unit") or "un"

        original_unit_multiplier = None
        param_unit_multiplier = context.parameters.get("unit_multiplier")
        if param_unit_multiplier is not None:
            if not isinstance(param_unit_multiplier, (int, float)) or isinstance(param_unit_multiplier, bool):
                return TextResponse(data=json.dumps({"error": "Parâmetro 'unit_multiplier' inválido. Deve ser um número."}, ensure_ascii=False))
            original_unit_multiplier = float(param_unit_multiplier)
            if original_unit_multiplier < 0:
                return TextResponse(data=json.dumps({"error": "Parâmetro 'unit_multiplier' deve ser um número positivo."}, ensure_ascii=False))

        if original_sku or original_price:
            print(f"Filtros ativados: sku_original={original_sku}, preço_original={original_price}, quantity={original_quantity}, unit={original_unit}, unit_multiplier={original_unit_multiplier}")
        else:
            print("Parâmetros de filtro não informados - busca sem filtro de preço e sem exclusão de SKU")
        
        # Processar múltiplos produtos separados por vírgula
        product_names = [p.strip() for p in product_name.split(",") if p.strip()]

        STOPWORDS = {"de", "da", "do", "das", "dos", "e", "com", "em", "para", "a", "o", "um", "uma"}

        def search_substring(name: str, max_words: int = 1) -> str:
            """Extrai apenas o substantivo principal (1ª palavra relevante) para busca mais ampla.
            Ex: Presunto Cozido->Presunto, Cacho de Banana Prata->Banana, Água Mineral->Água."""
            if " de " in name.lower():
                name = name.split(" de ", 1)[1]
            words = [w for w in name.split() if w.lower() not in STOPWORDS]
            return (words[0] if words else name).strip()

        base_url = "https://hmlzaffari.myvtex.com"
        url = f"{base_url}/api/io/_v/api/intelligent-search/product_search/"

        # Buscar cada produto e consolidar resultados (usando substring na query)
        all_products = {}

        for pname in product_names:
            search_term = search_substring(pname)
            print(f"Busca com substring: '{pname}' -> '{search_term}'")
            products_found, _ = self.intelligent_search(
                product_name=search_term, 
                url=url,
                original_price=original_price,
                original_quantity=original_quantity,
                original_unit=original_unit,
                original_sku=original_sku,
            )
            all_products.update(products_found)

        print(f"Total de produtos encontrados: {len(all_products)}")
        return TextResponse(data=json.dumps(all_products, ensure_ascii=False, indent=2))

    def intelligent_search(
        self, 
        product_name: str, 
        url: str,
        original_price: Optional[float] = None,
        original_quantity: Optional[float] = None,
        original_unit: Optional[str] = None,
        original_sku: Optional[str] = None,
    ) -> Tuple[dict, list]:
        """
        Searches for products by name and collects detailed information.
        Aplica:
        - Filtro de 20% se original_price estiver disponível.
        - Filtro para NUNCA sugerir o mesmo SKU do produto faltante (original_sku).

        Args:
            product_name (str): Name of the product to search for
            url (str): Base URL for the search
            original_price (float, optional): Preço original do produto faltante (para filtro de 20%)
            original_quantity (float, optional): Quantidade original do produto faltante
            original_unit (str, optional): Unidade do produto original ("kg" ou "un")
            original_sku (str, optional): SKU do produto faltante (usado para nunca sugerir o mesmo produto)

        Returns:
            tuple: (Dictionary with product names as keys and their details including all variations, List of SKU IDs found)
        """
        products_structured = {}
        skus_found = []
        produtos_rejeitados_por_20 = 0

        search_url = f"{url}?query={product_name}&hideUnavailableItems=true"

        try:
            response = requests.get(search_url)
            response.raise_for_status()
            response_data = response.json()
            products = response_data.get("products", [])

            print(f"API retornou {len(products)} produtos para '{product_name}'")

            for product in products:
                if not product.get("items"):
                    print(f"Skipping product {product.get('productId')} due to missing items.")
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
                    
                    # Obter measurement_unit do item (pode estar no item ou precisar inferir)
                    measurement_unit = item.get("measurementUnit", "").lower()
                    if not measurement_unit:
                        # Tentar inferir do nome ou usar fallback
                        sku_name_lower = sku_name.lower() if sku_name else ""
                        if "kg" in sku_name_lower or "kilograma" in sku_name_lower:
                            measurement_unit = "kg"
                        else:
                            measurement_unit = "un"  # Fallback padrão

                    # Aplicar filtro para NUNCA sugerir o próprio produto original
                    if original_sku is not None and sku_id is not None:
                        if str(sku_id) == str(original_sku):
                            print(f"SKU {sku_id} ignorado: é o mesmo SKU do produto faltante (original_sku={original_sku})")
                            continue

                    # Aplicar filtro de 20% se original_price estiver disponível
                    if original_price and original_price > 0 and price > 0:
                        price_increase_percent = ((price - original_price) / original_price) * 100
                        if price_increase_percent > 20.0:
                            produtos_rejeitados_por_20 += 1
                            print(f"Produto {sku_id} rejeitado: preço {price} ultrapassa 20% do original {original_price} (aumento: {price_increase_percent:.2f}%)")
                            continue  # Pular este produto

                    if sku_id:
                        variation = {
                            "sku_id": sku_id,
                            "sku_name": sku_name,
                            "variations": variation_item,
                            "price": price,  # Preço unitário
                            "measurement_unit": measurement_unit,  # Unidade de medida ("kg" ou "un")
                        }
                        variations.append(variation)
                        # Coletar SKU para retorno (mantido para compatibilidade)
                        if sku_id not in skus_found:
                            skus_found.append(sku_id)

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

            if produtos_rejeitados_por_20 > 0:
                print(f"Total de produtos rejeitados por ultrapassar 20% do preço original: {produtos_rejeitados_por_20}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {product_name}: {e}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response for {product_name}: {e}")

        return products_structured, skus_found
