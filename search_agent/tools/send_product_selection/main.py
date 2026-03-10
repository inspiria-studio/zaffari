from weni import Tool
from weni.context import Context
from weni.responses import TextResponse
import requests
import json
from typing import Dict, Any, Optional


class SendProductSelection(Tool):
    def calculate_replacement_quantity(
        self,
        original_price: float,
        original_quantity: float,
        replacement_price: float,
        original_unit: str,
        replacement_measurement_unit: Optional[str] = None,
    ) -> float:
        """
        Calcula a quantidade do produto substituto baseado nas regras de negócio.
        
        Regras:
        - Se preço_sub <= preço_orig: quantidade_sub = quantidade_orig
        - Se preço_sub > preço_orig: quantidade_sub = valor_total_orig / preço_sub
        - Valor total NUNCA pode ultrapassar o original
        - Preço unitário pode ser no máximo 20% maior (validação opcional - warning)
        
        Args:
            original_price: Preço unitário do produto original
            original_quantity: Quantidade do produto original
            replacement_price: Preço unitário do produto substituto
            original_unit: Unidade do produto original ("kg" ou "un")
            replacement_measurement_unit: Unidade do produto substituto (opcional, usar se disponível)
        
        Returns:
            float: Quantidade calculada do substituto
        
        Raises:
            ValueError: Se quantidade calculada não respeitar regras mínimas ou valor total ultrapassar
        """
        # Validar limite de 20% (opcional - warning, não bloquear)
        if original_price > 0:
            price_increase_percent = ((replacement_price - original_price) / original_price) * 100
            if price_increase_percent > 20.0:
                # Warning: preço ultrapassa 20%, mas ainda calcular quantidade
                # (a busca já deve ter filtrado, mas validar aqui também)
                print(f"AVISO: Preço do substituto ({replacement_price}) ultrapassa 20% do original ({original_price}). Aumento: {price_increase_percent:.2f}%")
        
        # Calcular valor total original (arredondado para 3 casas)
        original_total = round(original_price * original_quantity, 3)
        
        # Calcular quantidade
        if replacement_price <= original_price:
            # Caso 1: Preço menor ou igual - usar quantidade original
            qty_sub = round(original_quantity, 3)
        else:
            # Caso 2: Preço maior - calcular baseado no valor total
            qty_ideal = original_total / replacement_price
            
            # Determinar se é produto por peso
            # Usar measurement_unit do substituto se disponível, senão usar unit do original
            unit_to_check = replacement_measurement_unit.lower() if replacement_measurement_unit else original_unit.lower()
            is_weight_product = (unit_to_check == "kg") or (original_quantity < 1)
            
            if is_weight_product:
                # Produtos por peso: manter 3 casas decimais
                qty_sub = round(qty_ideal, 3)
            else:
                # Produtos por unidade: usar inteiro (arredondar para baixo)
                qty_sub = int(qty_ideal)
                if qty_sub <= 0:
                    qty_sub = 1
        
        # Validar valor total após cálculo inicial
        valor_total_final = round(replacement_price * qty_sub, 3)
        if valor_total_final > original_total:
            # Reduzir proporcionalmente se ainda ultrapassar
            reduction = 0.001 if is_weight_product else 1.0
            qty_sub = round(max(0, qty_sub - reduction), 3 if is_weight_product else 0)
            valor_total_final = round(replacement_price * qty_sub, 3)
        
        # Validar quantidade mínima aceitável
        min_qty_threshold = 0.001 if original_quantity < 1 else 1.0
        if qty_sub < min_qty_threshold:
            raise ValueError(
                f"Quantidade calculada {qty_sub} é menor que o mínimo aceitável {min_qty_threshold}. "
                f"Valor total original: {original_total}, Preço substituto: {replacement_price}"
            )
        
        # Validação final: garantir que valor total NUNCA ultrapasse o original
        if valor_total_final > original_total:
            raise ValueError(
                f"Valor total {valor_total_final} ultrapassa o original {original_total}. "
                f"Quantidade calculada: {qty_sub}, Preço substituto: {replacement_price}"
            )
        
        print(f"Quantidade calculada: {qty_sub} (original: {original_quantity}, preço_orig: {original_price}, preço_sub: {replacement_price}, valor_total_orig: {original_total}, valor_total_final: {valor_total_final})")
        
        return qty_sub
    
    def get_item_faltando_from_contact(self, context: Context) -> Optional[Dict[str, Any]]:
        """
        Busca o campo item_faltando_jornada direto na API de contatos da Weni.
        OBRIGATÓRIO para jornada desestruturada.
        
        Args:
            context: Contexto da Weni (usado apenas para descobrir o URN do contato)
        
        Returns:
            dict: Dados do item faltante ou None se não encontrado
        """
        # 1) Descobrir URN do contato a partir do contexto
        urn = context.contact.get("urn","")
        
        
        if not urn:
            print("URN do contato não encontrada no contexto para buscar item_faltando_jornada na Weni")
            return None
        
        # 2) Consultar contato na API da Weni (mesma abordagem de zaffari_substitu_automatica.py)
        try:
            url = f"https://flows.weni.ai/api/v2/contacts.json?urn={urn}"
            headers = {"Authorization": "token 820e33e8b3ba173c4330ec7f794a0c45a9a5cc70"}
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            contact_data = results[0] if results else None
            
            if not contact_data or not isinstance(contact_data, dict):
                print(f"Contato não encontrado na Weni para URN={urn}")
                return None
            
            contact_fields = contact_data.get("fields") or {}
            item_faltando_json = contact_fields.get("item_faltando_jornada")
            if not item_faltando_json:
                print(f"Campo item_faltando_jornada não encontrado nos fields do contato URN={urn}")
                return None
            
            if isinstance(item_faltando_json, str):
                return json.loads(item_faltando_json)
            elif isinstance(item_faltando_json, dict):
                return item_faltando_json
            else:
                print(f"Formato inesperado de item_faltando_jornada: {type(item_faltando_json)}")
                return None
        
        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Erro ao buscar item_faltando_jornada na API da Weni: {e}")
            return None
    
    
    def execute(self, context: Context) -> TextResponse:
        """
        Envia para a Instaleap a decisão do cliente em duas modalidades:
        - Substituição: envia o produto escolhido como substituto, com quantidade calculada automaticamente.
        - Remoção sem substituição: registra que o cliente optou por não substituir o item ("CLIENTE NÃO SUBSTITUIR").
        
        Args:
            context: Contexto da Weni com os parâmetros:
                - order_id: ID do pedido
                - product_id_original: ID do produto original que foi removido
                - product_id_replacement: ID do produto escolhido como substituto
                - replacement_price: Preço unitário do produto substituto (OBRIGATÓRIO)
                - replacement_measurement_unit: Unidade de medida do substituto (opcional)
                - quantity: Quantidade do produto substituto (OPCIONAL - será IGNORADA se item_faltando_jornada estiver disponível)
                - no_replacement: Se verdadeiro, registra apenas remoção do item sem substituição ("CLIENTE NÃO SUBSTITUIR")
        
        Returns:
            TextResponse com o resultado da operação
        """
        # Detectar se é caso de remoção sem substituição
        raw_no_replacement = context.parameters.get("no_replacement")
        no_replacement = False
        if isinstance(raw_no_replacement, bool):
            no_replacement = raw_no_replacement
        elif raw_no_replacement is not None:
            no_replacement = str(raw_no_replacement).strip().lower() in ["true", "1", "yes", "sim"]
        
        # Parâmetros básicos
        order_id = context.parameters.get("order_id", "")
        product_id_original = context.parameters.get("product_id_original", "")
        
        # order_id é obrigatório em qualquer modo
        if not order_id:
            return TextResponse(data=json.dumps({
                "success": False,
                "error": "order_id é obrigatório"
            }, ensure_ascii=False))
        
        # Caminho 1: Remoção sem substituição ("CLIENTE NÃO SUBSTITUIR")
        if no_replacement:
            if not product_id_original:
                return TextResponse(data=json.dumps({
                    "success": False,
                    "error": "product_id_original é obrigatório para registrar remoção sem substituição"
                }, ensure_ascii=False))
            
            try:
                sucesso_ruptura = self.send_instaleap_external_data(
                    order_id=order_id,
                    product_id=product_id_original,
                    ruptura_message="CLIENTE NÃO SUBSTITUIR"
                )
                
                if not sucesso_ruptura:
                    return TextResponse(data=json.dumps({
                        "success": False,
                        "error": "Erro ao registrar remoção do item sem substituição na Instaleap"
                    }, ensure_ascii=False))
                
                # Fazer pop na fila após sucesso do envio de ruptura
                pop_success = self.pop_fila_from_codeaction(order_id)
                
                return TextResponse(data=json.dumps({
                    "success": True,
                    "mode": "no_replacement",
                    "message": "Remoção do item sem substituição registrada com sucesso na Instaleap",
                    "order_id": order_id,
                    "product_id_original": product_id_original,
                    "ruptura_message": "CLIENTE NÃO SUBSTITUIR",
                    "pop_fila": pop_success
                }, ensure_ascii=False))
            except Exception as e:
                return TextResponse(data=json.dumps({
                    "success": False,
                    "error": f"Erro inesperado ao registrar remoção sem substituição: {str(e)}"
                }, ensure_ascii=False))
        
        # Caminho 2: Substituição com cálculo automático de quantidade
        # 1. Ler item_faltando_jornada do contato
        item_faltando = self.get_item_faltando_from_contact(context)
        
        # 2. Se item_faltando_jornada NÃO estiver disponível, retornar erro
        if not item_faltando:
            return TextResponse(data=json.dumps({
                "success": False,
                "error": "Campo 'item_faltando_jornada' não encontrado no contato. Este campo é obrigatório para calcular a quantidade do substituto na jornada desestruturada."
            }, ensure_ascii=False))
        
        # 3. Obter parâmetros específicos de substituição
        product_id_replacement = context.parameters.get("product_id_replacement", "")
        replacement_price = context.parameters.get("replacement_price")
        replacement_measurement_unit = context.parameters.get("replacement_measurement_unit")
        
        if not product_id_original:
            return TextResponse(data=json.dumps({
                "success": False,
                "error": "product_id_original é obrigatório para substituição"
            }, ensure_ascii=False))
        
        if not product_id_replacement:
            return TextResponse(data=json.dumps({
                "success": False,
                "error": "product_id_replacement é obrigatório para substituição"
            }, ensure_ascii=False))
        
        # 4. OBRIGATÓRIO: Obter preço do substituto via parâmetro do agente
        if not replacement_price:
            return TextResponse(data=json.dumps({
                "success": False,
                "error": "Parâmetro 'replacement_price' é obrigatório para substituição. O preço do produto substituto deve ser fornecido pelo agente através deste parâmetro."
            }, ensure_ascii=False))
        
        # Converter replacement_price para float
        try:
            replacement_price = float(replacement_price)
        except (ValueError, TypeError):
            return TextResponse(data=json.dumps({
                "success": False,
                "error": f"Preço do substituto inválido: {replacement_price}"
            }, ensure_ascii=False))
        
        # 5. Calcular quantidade usando item_faltando_jornada
        # IGNORAR quantity fornecida manualmente (sempre usar cálculo automático)
        try:
            # Validar dados do item_faltando_jornada
            original_price = float(item_faltando.get("price", 0))
            original_quantity = float(item_faltando.get("quantity", 0))
            original_unit = item_faltando.get("unit", "un")
            
            if original_price <= 0:
                return TextResponse(data=json.dumps({
                    "success": False,
                    "error": f"Preço original inválido no item_faltando_jornada: {original_price}"
                }, ensure_ascii=False))
            
            if original_quantity <= 0:
                return TextResponse(data=json.dumps({
                    "success": False,
                    "error": f"Quantidade original inválida no item_faltando_jornada: {original_quantity}"
                }, ensure_ascii=False))
            
            # Calcular quantidade automaticamente
            calculated_quantity = self.calculate_replacement_quantity(
                original_price=original_price,
                original_quantity=original_quantity,
                replacement_price=replacement_price,
                original_unit=original_unit,
                replacement_measurement_unit=replacement_measurement_unit
            )
            
            quantity = calculated_quantity  # SEMPRE usar quantidade calculada
            
            print(f"Quantidade calculada automaticamente: {quantity} (ignorando quantity fornecida manualmente)")
            
        except ValueError as e:
            # Se cálculo falhar, retornar erro
            return TextResponse(data=json.dumps({
                "success": False,
                "error": f"Erro ao calcular quantidade automaticamente: {str(e)}"
            }, ensure_ascii=False))
        except (KeyError, TypeError) as e:
            return TextResponse(data=json.dumps({
                "success": False,
                "error": f"Erro ao processar dados do item_faltando_jornada: {str(e)}"
            }, ensure_ascii=False))
        
        # 6. Enviar sugestão de substituição para a Instaleap
        try:
            success = self.send_replacement_suggestion(
                order_id=order_id,
                product_id_original=product_id_original,
                product_id_replacement=product_id_replacement,
                quantity=quantity
            )
            
            if success:
                # Fazer pop na fila após sucesso do envio
                pop_success = self.pop_fila_from_codeaction(order_id)
                
                return TextResponse(data=json.dumps({
                    "success": True,
                    "mode": "replacement",
                    "message": f"Produto {product_id_replacement} enviado com sucesso como substituto para o produto {product_id_original} no pedido {order_id}",
                    "order_id": order_id,
                    "product_id_original": product_id_original,
                    "product_id_replacement": product_id_replacement,
                    "quantity": quantity,
                    "quantity_calculated": True,
                    "original_price": original_price,
                    "original_quantity": original_quantity,
                    "replacement_price": replacement_price,
                    "pop_fila": pop_success
                }, ensure_ascii=False))
            else:
                return TextResponse(data=json.dumps({
                    "success": False,
                    "error": "Erro ao enviar sugestão de substituição para a Instaleap"
                }, ensure_ascii=False))
                
        except Exception as e:
            return TextResponse(data=json.dumps({
                "success": False,
                "error": f"Erro inesperado: {str(e)}"
            }, ensure_ascii=False))
    
    def send_replacement_suggestion(
        self, 
        order_id: str, 
        product_id_original: str, 
        product_id_replacement: str, 
        quantity: float
    ) -> bool:
        """
        Envia sugestão de substituição escolhida para a API da Zaffari (ambiente QA).
        
        POST https://hml-api.zaffari.com.br/ecommerce/integration-matrix/api/v1/flow/orderProductReplacementSugestion
        Body: {
          "orderId": "string",
          "productIdOriginal": "string",
          "productIdReplacement": "string",
          "quantity": 1
        }
        
        Args:
            order_id (str): ID do pedido
            product_id_original (str): ID do produto original que foi removido
            product_id_replacement (str): ID do produto escolhido como substituto
            quantity (float): Quantidade do produto substituto
        
        Returns:
            bool: True se o envio foi bem-sucedido, False caso contrário
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
                "Ocp-Apim-Subscription-Key": "5400fb5a63a945b1a2bbab6086a94c71"
            }
            
            print(f"Enviando sugestão de substituição: {json.dumps(payload, indent=2)}")
            
            # Timeout reduzido para evitar timeout do contexto de requisição
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            
            success = 200 <= resp.status_code < 300
            
            if success:
                print(f"Sugestão de substituição enviada com sucesso. Status: {resp.status_code}")
            else:
                print(f"Erro ao enviar sugestão de substituição. Status: {resp.status_code}, Response: {resp.text[:200]}")
            
            return success
            
        except requests.exceptions.Timeout:
            print("Timeout ao enviar sugestão de substituição")
            return False
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição ao enviar sugestão de substituição: {e}")
            return False
        except Exception as e:
            print(f"Erro inesperado ao enviar sugestão de substituição: {e}")
            return False
    
    def send_instaleap_external_data(
        self,
        order_id: str,
        product_id: str,
        ruptura_message: str
    ) -> bool:
        """
        Envia informação de ruptura (remoção sem substituição) para a API da Zaffari (Instaleap).
        
        POST https://hml-api.zaffari.com.br/ecommerce/integration-matrix/api/v1/flow/instaleapExternalData
        Body:
        {
          "orderId": "string",
          "productId": "string",
          "messages": {
            "ruptura": "string"
          }
        }
        """
        # Validar que product_id não está vazio
        if not product_id or str(product_id).strip() == "":
            print(f"send_instaleap_external_data: ERRO product_id está vazio (order_id={order_id})")
            return False
        
        product_id = str(product_id).strip()
        order_id = str(order_id).strip() if order_id else ""
        
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
                "Ocp-Apim-Subscription-Key": "5400fb5a63a945b1a2bbab6086a94c71"
            }
            
            print(f"Enviando ruptura Instaleap: {json.dumps(payload, ensure_ascii=False)}")
            
            # Timeout reduzido para evitar timeout do contexto de requisição
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            
            status_code = resp.status_code
            print(f"Resposta Instaleap ruptura: status_code={status_code}, body={resp.text[:300] if resp.text else ''}")
            
            return 200 <= status_code < 300
        except requests.exceptions.Timeout:
            print("Timeout ao enviar ruptura para Instaleap")
            return False
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição ao enviar ruptura para Instaleap: {e}")
            return False
        except Exception as e:
            print(f"Erro inesperado ao enviar ruptura para Instaleap: {e}")
            return False
    
    def pop_fila_from_codeaction(self, numero_pedido: str) -> bool:
        """
        Faz POP na fila do Google Sheets através do CodeAction.
        Remove e retorna o primeiro JSON da fila de um pedido específico.
        
        Args:
            numero_pedido (str): Número do pedido
            
        Returns:
            bool: True se o pop foi bem-sucedido, False caso contrário
        """
        try:
            # URL do CodeAction de fila
            codeaction_url = "https://code-actions.weni.ai/action/endpoint/692034097a06b1c824249d9d"
            
            # Adicionar numeroPedido como query parameter
            url_with_params = f"{codeaction_url}?numeroPedido={numero_pedido}"
            
            payload = {
                "acao": "pop",
                "numeroPedido": str(numero_pedido)
            }
            
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Python-Requests/2.28.0'
            }
            
            print(f"Fazendo pop na fila para pedido {numero_pedido}")
            
            # Timeout reduzido para evitar timeout do contexto de requisição
            response = requests.post(url_with_params, json=payload, headers=headers, timeout=30)
            
            # Verificar status antes de fazer raise_for_status para capturar resposta de erro
            if response.status_code >= 400:
                print(f"Erro HTTP {response.status_code} ao fazer pop na fila: {response.text[:200]}")
                return False
            
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") == "ok":
                json_removido = result.get("jsonRemovido")
                print(f"Pop realizado com sucesso. JSON removido: {json_removido is not None}")
                return True
            elif result.get("status") in ["vazio_ou_inexistente", "vazio", "fila_vazia", "nao_encontrado"]:
                print(f"Fila vazia ou pedido não encontrado. Status: {result.get('status')}")
                return True  # Não é erro, apenas informação
            else:
                print(f"Erro ao fazer pop na fila. Status: {result.get('status')}, Mensagem: {result.get('mensagem', 'N/A')}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição ao fazer pop na fila: {e}")
            return False
        except json.JSONDecodeError as e:
            print(f"Erro ao fazer parse da resposta do pop: {e}")
            return False
        except Exception as e:
            print(f"Erro inesperado ao fazer pop na fila: {e}")
            return False

