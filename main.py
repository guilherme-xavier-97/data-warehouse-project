from scripts.bronze.bronze_layer import run_bronze_layer
from scripts.silver.silver_crm_cust_info import run_crm_cust_info
from scripts.silver.silver_crm_prd_info import run_crm_prd_info

def main():
    print("Iniciando Pipeline de Dados...")
    
    # Chama a Bronze
    run_bronze_layer()

    #Transformatinos in the crm_cust_info
    run_crm_cust_info()

    #Transformatinos in the crm_prd_info
    run_crm_prd_info()
    
    print("Pipeline has fineshed sucessfully!")

if __name__ == "__main__":
    main()