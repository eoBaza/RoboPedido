import psycopg2
import cx_Oracle as oracle
import pymysql
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
import os
from queries import *
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

with open('config.json') as f:
    config = json.load(f)

class DatabaseManager:
    def __init__(self):
        self.tns_admin = r"C:\oracle\product\11.2.0\client_1\network\admin"
        self.oracle_conn = None
        self.pg_conn = None
        self.mysql_conn = None
        self.log_file = "monitoramento_log.txt"
        self._init_log()

    def _init_log(self):
        """Inicializa o arquivo de log"""
        with open(self.log_file, 'w', encoding="utf-8") as log:
            log.write(f"=== INICIO DO PROCESSO D0 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")

    def _log(self, message: str, print_to_console: bool = True):
        """Registra mensagem no log"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, 'a', encoding="utf-8") as log:
            log.write(log_entry)
        
        if print_to_console:
            print(log_entry.strip())

    def get_filiais_from_oracle(self) -> List[int]:
        """Obtém lista de filiais do Oracle"""
        try:
            self.connect_to_oracle() 
            
            with self.oracle_conn.cursor() as cursor:
                cursor.execute(consulta_filias())
                return [int(row[0]) for row in cursor.fetchall()]
                
        except Exception as e:
            self._log(f"ERRO Oracle - Busca de filiais: {e}")
            raise

    def connect_to_pg(self, filial: int) -> bool:
        """Conecta ao PostgreSQL para uma filial específica"""
        filial_padded = f"{filial:03d}"
        host = f"qql{filial_padded}00.qq"
        try:
            self.pg_conn = psycopg2.connect(
                host=host,
                database=config["pg"]["database"],
                user=config["pg"]["user"],
                password=config["pg"]["password"],
                port=config["pg"]["port"]
            )
            return self.pg_conn
        except Exception as e:
            self._log(f"ERRO PG - Conexão filial {filial}: {e}")
            return False

    def connect_to_oracle(self):
        os.environ["TNS_ADMIN"] = self.tns_admin
        if self.oracle_conn is None:
            self.oracle_conn = oracle.connect(
                user=config["oracle"]["user"],
                password=config["oracle"]["password"],
                dsn=config["oracle"]["database"]
            )
            self._log("Conectado ao Oracle.")
        else:
            # Opcional: testar se a conexão ainda está válida
            try:
                with self.oracle_conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM DUAL")
            except:
                self._log("Reconectando Oracle porque a conexão anterior caiu.")
                self.oracle_conn.close()
                self.oracle_conn = oracle.connect(
                    user=config["oracle"]["user"],
                    password=config["oracle"]["password"],
                    dsn=config["oracle"]["database"]
                )

    def connect_to_mysql(self) -> bool:
        """Conecta ao MySQL para logar os erros"""
        try:
            self.mysql_conn = pymysql.connect(
                host=config["mysql"]["host"],
                user=config["mysql"]["user"],
                password=config["mysql"]["password"],
                database=config["mysql"]["database"],
                port=3306
            )
            return True
        except Exception as e:
            self._log(f"ERRO MySQL - Conexão: {e}")
            return False

    def parse_payload(self, payload: str) -> Dict[str, Any]:
        """Extrai dados do payload JSON"""
        try:
            data = json.loads(payload)
            data_data = data.get("data", {})
            # Verifica se é um payload de VENDA (contém legacyData)
            if "legacyData" in data_data:
                legacy_data = data_data.get("legacyData", [{}])
                first_legacy = legacy_data[0] if legacy_data else {}
                
                pdv_data = first_legacy.get("pdvData", {})
                cupom_doc = first_legacy.get("cupomDoc", {})
                
                return {
                    "pedido": first_legacy.get("id_pedido_pg"),
                    "nr_pdv": first_legacy.get("nr_pdv"),
                    "nr_cupom": first_legacy.get("nr_cupom"),
                    "vl_total": first_legacy.get("vl_cupom"),
                    "filial": first_legacy.get("filial_saida"),
                    "dt_cupom": first_legacy.get("dt_cupom"),
                    "id_cupom_pg": first_legacy.get("id_cupom_pg"),
                    "status": first_legacy.get("status"),
                    "vendedor": first_legacy.get("id_ven")
                }
            # Caso contrário, assume que é um payload de CORRESPONDENTE_BANCARIO
            cb_data = data_data.get("cb", {})
            corresp_bancario = cb_data.get("CORRESPONDENTE_BANCARIO", [{}])
            first_item = corresp_bancario[0] if corresp_bancario else {}
            cupom_complemento = first_item.get("cupomComplemento", {})
            
            return {
                "pedido": None,  # Não disponível neste tipo de payload
                "nr_pdv": cupom_complemento.get("pdv"),
                "nr_cupom": cupom_complemento.get("cupom"),
                "vl_total": cupom_complemento.get("valor"),
                "filial": cupom_complemento.get("filial"),
                "dt_cupom": cupom_complemento.get("dt_cupom"),
                "id_cupom_pg": data_data.get("id_cupom_pg"),
                "status": None,
                "vendedor": None
            }
        except Exception as e:
            self._log(f"ERRO ao parsear payload: {e}", print_to_console=False)
            return {}

    def insert_erro_mysql(self, filial: int, nr_cupom: int, evento: Dict[str, Any]) -> bool:
        """Insere registro de erro no MySQL"""
        if not self.mysql_conn:
            if not self.connect_to_mysql():
                return False

        payload_data = self.parse_payload(evento['payload'])
        id_chave = payload_data.get('id_cupom_pg') or payload_data.get('pedido')
        try:
            with self.mysql_conn.cursor() as cursor:
                sql_check , params = validar_item_duplicadoD0(filial, nr_cupom)
                cursor.execute(sql_check, params)
                resultado = cursor.fetchone()
                if resultado and resultado[0] > 0:
                    self._log(f"Erro já registrado para filial {filial}, pedido {id_chave}, pulando inserção.")
                    return False
                sql = inserir_DO()
                cursor.execute(sql, (
                    filial,
                    payload_data.get('pedido'),
                    payload_data.get('nr_pdv'),
                    evento.get('id_cupom'),
                    payload_data.get('nr_cupom'),
                    evento.get('log'), 
                    payload_data.get('vl_total'),
                    evento.get('dh_inclusao'),
                    evento.get('id_evento'),
                    'NOK'
                ))
            self.mysql_conn.commit()
            return True
        except Exception as e:
            self._log(f"ERRO MySQL - Insert filial {filial}: {e}")
            return False

    def process_filiais(self):
        """Processa todas as filiais e loga erros no MySQL"""
        try:
            if not self.connect_to_mysql():
                raise Exception("Não foi possível conectar ao MySQL")

            filiais = self.get_filiais_from_oracle()
            self._log(f"Total de filiais a processar: {len(filiais)}")

            total_erros = 0

            for filial in filiais:
                try:
                    if not self.connect_to_pg(filial):
                        continue

                    with self.pg_conn.cursor() as pg_cursor:
                        pg_cursor.execute(querie_business())
                        eventos = pg_cursor.fetchall()

                        if not eventos:
                            self._log(f"Filial {filial}: Nenhum evento com erro")
                            continue

                        col_names = [desc[0] for desc in pg_cursor.description]
                        eventos_dict = [dict(zip(col_names, row)) for row in eventos]

                        # Agrupamento por ID (id_pedido ou id_cupom)
                        grupos = {}
                        for evento in eventos_dict:
                            payload_info = self.parse_payload(evento["payload"])
                            tipo = "pedido" if payload_info.get("pedido") else "id_cupom_pg"
                            id_chave = payload_info.get("pedido") or payload_info.get("id_cupom_pg")
                            if not id_chave:
                                continue
                            if id_chave not in grupos:
                                grupos[id_chave] = []
                            grupos[id_chave].append({**evento, **payload_info})

                        erros_para_inserir = []

                        for chave, eventos_agrupados in grupos.items():
                            evento_base = eventos_agrupados[0]  # pega o primeiro evento como referência
                            tipo = "pedido" if evento_base.get("pedido") else "id_cupom_pg"
                            nr_cupom = evento_base.get("nr_cupom")

                            # Verifica se já houve evento de sucesso no banco para essa chave
                            chave_payload = "id_pedido_pg" if tipo == "pedido" else "id_cupom_pg"
                            query_valid, params = validar_busines_event(chave_payload, chave)
                            pg_cursor.execute(query_valid, params)
                            resultados = pg_cursor.fetchall()

                            if any(row[1].strip().lower() == "sucesso" for row in resultados):
                                self._log(f"Filial {filial}: Ignorado {tipo} {chave}, pois já teve evento com sucesso.")
                                continue

                            erros_para_inserir.append((filial, nr_cupom, evento_base))
                            self._log(
                                f"Filial {filial}: tipo {tipo} -> inserindo erro do evento {evento_base['log']} "
                                f"com status {evento_base.get('status_execucao')}, nr_cupom {nr_cupom}"
                            )

                        if erros_para_inserir:
                            for filial, nr_cupom, evento in erros_para_inserir:
                                self.insert_erro_mysql(filial, nr_cupom, evento)
                            total_erros += len(erros_para_inserir)

                except Exception as e:
                    self._log(f"ERRO Filial {filial}: {e}")
                finally:
                    if self.pg_conn:
                        self.pg_conn.close()

            self._log(f"Processamento concluído. Total de erros logados: {total_erros}")

        except Exception as e:
            self._log(f"ERRO no processamento principal: {e}")
            raise
        finally:
            self.close_all()


    def limpar_eventos_redundantes(self):
        filiais = self.get_filiais_from_oracle()
        self._log(f"Iniciando limpeza paralela para {len(filiais)} filiais")

        with ThreadPoolExecutor(max_workers=min(10, len(filiais))) as executor:
            futures = [executor.submit(self.processar_filial_limpeza, filial) for filial in filiais]
            for future in as_completed(futures):
                resultado = future.result()
                self._log(resultado)

    def processar_filial_limpeza(self, filial):
        conn = None
        try:
            # Conexão local e isolada por thread
            conn = self.connect_to_pg(filial)
            if not conn:
                return f"[Filial {filial}] Erro ao conectar ao PostgreSQL"

            with conn.cursor() as cursor:
                cursor.execute(limpeza_linha_erro_completa())
                removidos = cursor.rowcount
                conn.commit()
                return f"[Filial {filial}] Removidos {removidos} eventos redundantes com erro"

        except Exception as e:
            if conn:
                conn.rollback()
            return f"[Filial {filial}] ERRO durante limpeza: {e}"

        finally:
            if conn:
                conn.close()

    def close_all(self):
        """Fecha todas as conexões abertas"""
        try:
            if self.oracle_conn:
                try:
                    self.oracle_conn.close()
                    self._log("Conexão Oracle fechada com sucesso.")
                except Exception as e:
                    self._log(f"Erro ao fechar conexão Oracle: {e}")
                finally:
                    self.oracle_conn = None

            if self.pg_conn:
                try:
                    self.pg_conn.close()
                    self._log("Conexão PostgreSQL fechada com sucesso.")
                except Exception as e:
                    self._log(f"Erro ao fechar conexão PostgreSQL: {e}")
                finally:
                    self.pg_conn = None

            if self.mysql_conn:
                try:
                    self.mysql_conn.close()
                    self._log("Conexão MySQL fechada com sucesso.")
                except Exception as e:
                    self._log(f"Erro ao fechar conexão MySQL: {e}")
                finally:
                    self.mysql_conn = None

            self._log("Todas conexões fechadas.")
        except Exception as e:
            self._log(f"ERRO inesperado ao fechar conexões: {e}")

    def mostrar_pedidos_pendentes(self):
        """Mostra os pedidos pendentes no D0"""
        try:
            if not self.mysql_conn:
                if not self.connect_to_mysql():
                    self._log("Falha ao conectar no MySQL.")
                    return []
                
            self.cursor = self.mysql_conn.cursor()
            self.cursor.execute(valida_D0())
            rows = self.cursor.fetchall()

            pedidos = []
            for row in rows:
                pedidos.append({
                    "filial": row[0],
                    "pedido": row[1],
                    "nr_cupom": row[4],
                    "id_cupom": row[3],
                    "id_evento": row[6],
                    "is_sap": row[7],
                })
            return pedidos

        except Exception as e:
            self._log(f"Erro ao buscar pedidos pendentes: {e}")
            return []

    def validar_D0(self):
        """ VALIDAR D0 para atualizar os eventos de venda e dívida """
        pedidos = self.mostrar_pedidos_pendentes()

        for p in pedidos:
            pedido = p["pedido"]
            filial = p["filial"]
            nr_cupom = p["nr_cupom"]
            id_cupom = p["id_cupom"]
            id_evento = p["id_evento"]
            is_sap = p["is_sap"]

            self._log(f"Processando Pedido {pedido} / Filial {filial}...")

            # Decide o tipo
            campo_pg = valor_pg = tipo = None

            if pedido is None and id_cupom is not None:
                campo_pg = "id_cupom_pg"
                valor_pg = id_cupom
                tipo = "divida"
            elif pedido is not None:
                campo_pg = "id_pedido_pg"
                valor_pg = pedido
                tipo = "venda"
            elif pedido is None and id_cupom is None:
                valor_pg = nr_cupom
                tipo = "credito_Pessoal"

            # 1️ Validação PG obrigatória
            if campo_pg and valor_pg:
                self.connect_to_pg(filial)
                pg_cursor = self.pg_conn.cursor()
                query, params = validar_busines_event(campo_pg, valor_pg)
                pg_cursor.execute(query, params)
                resultado_pg = pg_cursor.fetchall()
                pg_cursor.close()

                evento_sucesso = any(r[1].lower() == "sucesso" for r in resultado_pg)

                for r in resultado_pg:
                    self._log(f"Evento {tipo} PG: ID {r[0]} | Status: {r[1]}")

                if not evento_sucesso:
                    self._log(f"Evento {tipo} ainda não está com status SUCESSO no PG. Pulando pedido {pedido}.")
                    continue

            # 2️ Se PG estiver OK, validar tipo do pedido no Oracle (somente se for VENDA)
            tipo_pedido = None
            if tipo == "venda" and pedido:
                try:
                    tipo_info = self.validar_tipo_retira_posterior(pedido, filial)
                    if not tipo_info:
                        self._log(f"Tipo do Pedido {pedido} não encontrado. Ignorando...")
                        continue

                    tipo_pedido = tipo_info[2]  # "P" ou "R"
                    self._log(f"Tipo do Pedido: {tipo_pedido}")

                except Exception as e:
                    self._log_error_d0(f"Erro ao validar tipo do pedido no Oracle: {e}")
                    continue

            # 3️ Validar na WMB de acordo com o tipo
            if tipo == "venda":
                try:
                    resultado_wmb = None

                    if tipo_pedido == "P":
                        resultado_wmb = self.validar_wmb_posterior(pedido)
                    elif tipo_pedido == "R":
                        resultado_wmb = self.validar_cupom_wmb_event(filial, nr_cupom)

                    if resultado_wmb:
                        self._log(f"Subiu para WMB com sucesso: {resultado_wmb}")
                    else:
                        self._log(f"WMB não retornou resultado para pedido {pedido}")
                        continue

                except Exception as e:
                    self._log_error_d0(f"Erro ao validar WMB: {e}")
                    continue

            # 4️ Atualiza no MySQL se tudo OK
            try:
                if tipo == "divida":
                    self.cursor.execute(update_divida_D0(), (filial, id_cupom))
                elif tipo == "venda":
                    self.cursor.execute(update_venda_D0(), (filial, pedido))
                self.mysql_conn.commit()
                self._log(f"Pedido {pedido} atualizado no MySQL com sucesso.")
            except Exception as e:
                self._log(f"Erro ao atualizar no MySQL: {e}")

        return True

    def validar_wmb_posterior(self, pedido: int) -> List[int]:
        try:
        
            self.connect_to_oracle()
           

            query, params = validar_wmb_event(pedido)
            

            with self.oracle_conn.cursor() as cursor:
                cursor.execute(query, params)

                for row in cursor.fetchall():
                    self._log(f"WMB_ROW: {row[0]},Filial: {row[1]} Pedido: {row[2]}, Subiu: {row[3]}")
                    return [row[0], row[1], row[2], row[3]]

            return []

        except Exception as e:
            self._log(f"ERRO de rodar WMB: {e}")
            raise

    def validar_tipo_retira_posterior(self, pedido, filial):
    
        try:
            self.connect_to_oracle()
            
            query, params = tipo_pedido(pedido, filial)
            

            with self.oracle_conn.cursor() as cursor:
                cursor.execute(query, params)

                for row in cursor.fetchall():
                    self._log(f"Filial: {row[0]}, Pedido: {row[1]}, Tipo Pedido: {row[2]}")
                    return [row[0], row[1], row[2]]

            return []

        except Exception as e:
            self._log(f"ERRO de rodar WMB: {e}")
            raise
    
    def validar_cupom_wmb_event(self, filail, cupom):
        try:
            self.connect_to_oracle()
            
            query, params = validar_cupom_wmb_event(filail, cupom)
            
            with self.oracle_conn.cursor() as cursor:
                cursor.execute(query, params)

                for row in cursor.fetchall():
                    self._log(f"Filial: {row[0]}, Cupom: {row[1]}, Subiu: {row[2]}, Pedido Filho: {row[3]}, Data: {row[4]}")
                    return [row[0], row[1], row[2], row[3], row[4]]

            return []
        except Exception as e:
            self._log(f"ERRO ao validar cupom WMB: {e}")
            raise
