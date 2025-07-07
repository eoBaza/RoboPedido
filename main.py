from DataBase import DatabaseManager
import time
from datetime import datetime

def validar_pedidos_d0(monitor):
    monitor._log("INICIANDO VALIDAÇÃO D0...")
    monitor.validar_D0()
    if monitor.validar_D0() is None:
        monitor._log("Nenhum pedido D0 encontrado para validação.")
        monitor._log("\n###############VALIDAÇÃO D0 CONCLUÍDA.################\n")
    monitor._log("\n###############VALIDAÇÃO D0 CONCLUÍDA.################\n")

def processar_pedidos_d0(monitor):
    monitor._log("\n==================\nPROCESSANDO FILIAIS...\n==================")
    monitor.process_filiais()
    monitor._log("\n########################\nPROCESSAMENTO DE FILIAIS CONCLUÍDO.\n######################### \n")

def main():
    while True:
        monitor = DatabaseManager()
        try:
            monitor._log("===== INICÍO DO CÍCLO DE MONITORAMENTO =====")
            monitor.start_time = time.time()
            validar_pedidos_d0(monitor)
            processar_pedidos_d0(monitor)
            validar_pedidos_d0(monitor)
            monitor._log("####### CÍCLO DE MONITORAMENTO CONCLUÍDO ########")
        except Exception as e:
            monitor._log_error_d0(f"ERRO na execução do cíclo: {e}")
        finally:
            monitor._log("Fechando conexões e aguardando próximo cíclo...")
            monitor.close_all()
            monitor._log("Aguardando 10 minutos para o próximo cíclo...\n")
            time.sleep(600) 

if __name__ == "__main__":
    main()