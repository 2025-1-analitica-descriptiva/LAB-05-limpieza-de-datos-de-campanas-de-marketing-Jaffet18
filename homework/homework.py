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

    import pandas as pd
    import zipfile
    import os
    from pathlib import Path

    def clean_job(job):
        """Limpia la columna job reemplazando caracteres no deseados"""
        if pd.isna(job):
            return job
        return job.replace(".", "").replace("-", "_")

    def clean_education(education):
        """Limpia la columna education y maneja valores desconocidos"""
        if pd.isna(education) or education == "unknown":
            return pd.NA
        return education.replace(".", "_")

    def transform_data(input_zip_folder, output_folder):
        # Crear la carpeta de salida si no existe
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        
        # Inicializar DataFrames para cada archivo de salida
        client_df = pd.DataFrame()
        campaign_df = pd.DataFrame()
        economics_df = pd.DataFrame()
        
        # Procesar cada archivo ZIP en la carpeta de entrada
        for zip_file in Path(input_zip_folder).glob("*.zip"):
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # Asumimos que cada ZIP contiene exactamente un archivo CSV
                csv_file = zip_ref.namelist()[0]
                with zip_ref.open(csv_file) as file:
                    df = pd.read_csv(file)
                    
                    # Procesar datos para client.csv
                    client_data = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
                    client_data['job'] = client_data['job'].apply(clean_job)
                    client_data['education'] = client_data['education'].apply(clean_education)
                    client_data['credit_default'] = client_data['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
                    client_data['mortgage'] = client_data['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
                    
                    # Procesar datos para campaign.csv
                    campaign_data = df[['client_id', 'number_contacts', 'contact_duration', 
                                    'previous_campaign_contacts', 'previous_outcome', 
                                    'campaign_outcome', 'day', 'month']].copy()
                    campaign_data['previous_outcome'] = campaign_data['previous_outcome'].apply(
                        lambda x: 1 if x == 'success' else 0)
                    campaign_data['campaign_outcome'] = campaign_data['campaign_outcome'].apply(
                        lambda x: 1 if x == 'yes' else 0)
                    campaign_data['last_contact_day'] = pd.to_datetime(
                        '2022-' + campaign_data['month'].astype(str) + '-' + campaign_data['day'].astype(str),
                        format='%Y-%b-%d'
                    ).dt.strftime('%Y-%m-%d')
                    campaign_data.drop(['day', 'month'], axis=1, inplace=True)
                    
                    # Procesar datos para economics.csv
                    economics_data = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
                    
                    # Concatenar con los DataFrames principales
                    client_df = pd.concat([client_df, client_data], ignore_index=True)
                    campaign_df = pd.concat([campaign_df, campaign_data], ignore_index=True)
                    economics_df = pd.concat([economics_df, economics_data], ignore_index=True)
        
        # Eliminar duplicados por si hay solapamiento entre archivos ZIP
        client_df.drop_duplicates(subset=['client_id'], inplace=True)
        campaign_df.drop_duplicates(subset=['client_id'], inplace=True)
        economics_df.drop_duplicates(subset=['client_id'], inplace=True)
        
        # Guardar los DataFrames como archivos CSV
        client_df.to_csv(os.path.join(output_folder, 'client.csv'), index=False)
        campaign_df.to_csv(os.path.join(output_folder, 'campaign.csv'), index=False)
        economics_df.to_csv(os.path.join(output_folder, 'economics.csv'), index=False)

    # Uso de la función
    input_folder = "files/input"
    output_folder = "files/output"
    transform_data(input_folder, output_folder)

if __name__ == "__main__":
    clean_campaign_data()
