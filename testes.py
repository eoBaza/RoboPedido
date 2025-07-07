from DataBase import DatabaseManager
import time
def main ():
    while True:
        monitorar = DatabaseManager()
        try:
            
            # monitorar.mostrar_pedidos_pendentes()
            #monitorar.validar_D0()
            monitorar.limpar_eventos_redundantes()
        except Exception as e:
            print(f"Erro ao validar WMB: {e}")
            monitorar.close_all()
        finally:
            monitorar.close_all()
            time.sleep(500)

if __name__ == "__main__":
    main()