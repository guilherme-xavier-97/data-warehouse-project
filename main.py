from scripts.bronze.bronze_layer import run_bronze_layer
from scripts.silver.silver_crm_cust_info import run_crm_cust_info

def main():
    print("Iniciando Pipeline de Dados...")
    
    # Chama a Bronze
    run_bronze_layer()

    #Transformatinos in the crm_cust_info
    run_crm_cust_info()
    
    print("Pipeline finalizado com sucesso!")

if __name__ == "__main__":
    main()