"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import zipfile
import os
import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

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

    """
    Limpia los datos de una campaña de marketing realizada por un banco.
    Los datos se procesan desde archivos comprimidos en formato .zip y se dividen
    en tres archivos de salida: client.csv, campaign.csv y economics.csv.
    """
    input_dir = "files/input/"
    output_dir = "files/output/"

    # Asegurarse de que el directorio de salida exista
    os.makedirs(output_dir, exist_ok=True)

    # Columnas deseadas para los archivos de salida
    client_cols = ["client_id", "age", "job", "marital", "education", "credit_default", "mortage"]
    campaign_cols = ["client_id", "number_contacts", "contact_duration", "previous_campaing_contacts", "previous_outcome", "campaign_outcome", "last_contact_day"]
    economics_cols = ["client_id", "const_price_idx", "eurobor_three_months"]

    # DataFrames para almacenar los datos combinados
    client_data = []
    campaign_data = []
    economics_data = []

    # Procesar cada archivo zip
    for i in range(0, 10):
        zip_path = os.path.join(input_dir, f"bank-marketing-campaing-{i}.csv.zip")
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Asumimos que hay un solo archivo CSV dentro del zip
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file)

                # Limpiar y transformar los datos para client.csv
                client_df = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
                #client_df.rename(columns={"default": "credit_default", "housing": "mortage"}, inplace=True)
                client_df["job"] = client_df["job"].str.replace("\\.", "", regex=True).str.replace("-", "_", regex=True)
                client_df["education"] = client_df["education"].str.replace("\\.", "_", regex=True).replace("unknown", pd.NA)
                client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
                client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
                client_data.append(client_df)

                # Limpiar y transformar los datos para campaign.csv
                campaign_df = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]].copy()
                campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
                campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
                campaign_df["last_contact_date"] = pd.to_datetime(
                    campaign_df["day"].astype(str) + "-" + campaign_df["month"] + "-2022",
                    format="%d-%b-%Y"
                ).dt.strftime("%Y-%m-%d")
                campaign_df.drop(columns=["day", "month"], inplace=True)
                campaign_data.append(campaign_df)

                # Limpiar y transformar los datos para economics.csv
                economics_df = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
                economics_data.append(economics_df)

    # Combinar todos los datos y guardar los archivos de salida
    pd.concat(client_data).to_csv(os.path.join(output_dir, "client.csv"), index=False)
    pd.concat(campaign_data).to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    pd.concat(economics_data).to_csv(os.path.join(output_dir, "economics.csv"), index=False)



if __name__ == "__main__":
    clean_campaign_data()
    #campaign = pd.read_csv("files/output/campaign.csv")

    #print(campaign.shape)
    #print(campaign[campaign["previous_outcome"].map(lambda x: x == 0)].shape[0])
