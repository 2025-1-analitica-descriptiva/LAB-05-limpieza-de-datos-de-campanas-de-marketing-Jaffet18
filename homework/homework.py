"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerles un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """

    import os
    import zipfile
    import pandas as pd
    from datetime import datetime

    # Definición de rutas donde se almacenarán los resultados
    input_dir = os.path.join('.', 'files', 'input')
    output_dir = os.path.join('.', 'files', 'output')

    # Crear directorio de salida si no existe
    os.makedirs(output_dir, exist_ok=True)

    # Directorio de meses numérico
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    # Creación de DataFrames vacíos con las columnas específicas
    client_df = pd.DataFrame(columns=['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortage'])
    campaign_df = pd.DataFrame(columns=['client_id', 'number_contacts', 'contact_duration', 'previous_campaing_contacts', 
                                    'previous_outcome', 'campaign_outcome', 'last_contact_day'])
    economics_df = pd.DataFrame(columns=['client_id', 'const_price_idx', 'eurobor_three_months'])

    # Procesamiento de los achivos ZIP
    for zip_file in os.listdir(input_dir):
        if zip_file.endswith('.zip'):
            zip_path = os.path.join(input_dir, zip_file)
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        continue
                    
                    with z.open(csv_files[0]) as f:
                        df = pd.read_csv(f)
                        
                        # Clients
                        client_data = df[['client_id', 'age', 'job', 'marital', 'education', 
                                        'credit_default', 'mortgage']].copy()
                        client_data.rename(columns={'mortgage': 'mortage'}, inplace=True)
                        
                        # Limpiando datos según especificaciones
                        client_data['job'] = client_data['job'].str.replace('.', '').str.replace('-', '_')
                        client_data['education'] = client_data['education'].str.replace('.', '_').replace('unknown', pd.NA)
                        client_data['credit_default'] = client_data['credit_default'].apply(lambda x: 1 if str(x).lower() == 'yes' else 0)
                        client_data['mortage'] = client_data['mortage'].apply(lambda x: 1 if str(x).lower() == 'yes' else 0)
                        
                        client_df = pd.concat([client_df, client_data], ignore_index=True)
                        
                        # Campaing
                        if all(col in df.columns for col in ['month', 'day']):
                            campaign_data = df[['client_id', 'number_contacts', 'contact_duration', 
                                            'previous_campaing_contacts', 'previous_outcome', 
                                            'campaign_outcome', 'day', 'month']].copy()
                            campaign_data.rename(columns={'previous_campaign_contacts': 'previous_campaing_contacts'}, inplace=True)
                            
                            # Limpinando datos para Campaing según indicaciones
                            campaign_data['previous_outcome'] = campaign_data['previous_outcome'].apply(
                                lambda x: 1 if str(x).lower() == 'success' else 0
                            )
                            campaign_data['campaign_outcome'] = campaign_data['campaign_outcome'].apply(
                                lambda x: 1 if str(x).lower() == 'yes' else 0
                            )
                            
                            # Sustitución de fecha a numérica y creación de fecha
                            campaign_data['month'] = campaign_data['month'].str.lower().str[:3].map(month_map)
                            campaign_data['last_contact_day'] = campaign_data.apply(
                                lambda row: f"2022-{int(row['month']):02d}-{int(row['day']):02d}", axis=1
                            )
                            
                            campaign_data = campaign_data.drop(['day', 'month'], axis=1)
                            campaign_df = pd.concat([campaign_df, campaign_data], ignore_index=True)
                        
                        # Economics
                        economics_data = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
                        economics_data.rename(columns={
                            'cons_price_idx': 'const_price_idx',
                            'euribor_three_months': 'eurobor_three_months'
                        }, inplace=True)
                        
                        economics_df = pd.concat([economics_df, economics_data], ignore_index=True)
                        
            except Exception as e:
                continue

    # Eliminar duplicados por client_id
    client_df = client_df.drop_duplicates(subset=['client_id'])
    campaign_df = campaign_df.drop_duplicates(subset=['client_id'])
    economics_df = economics_df.drop_duplicates(subset=['client_id'])

    # Guardar los DataFrames
    client_df.to_csv(os.path.join(output_dir, 'client.csv'), index=False)
    campaign_df.to_csv(os.path.join(output_dir, 'campaign.csv'), index=False)
    economics_df.to_csv(os.path.join(output_dir, 'economics.csv'), index=False)

    #print(f"- client.csv: {len(client_df)} registros")
    #print(f"- campaign.csv: {len(campaign_df)} registros")
    #print(f"- economics.csv: {len(economics_df)} registros")

if __name__ == "__main__":
    clean_campaign_data()
