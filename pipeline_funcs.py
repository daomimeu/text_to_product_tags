import numpy as np
import pandas as pd
import re
import os


def clean_bo_report(file_path):
    df = pd.read_csv(file_path, skiprows=5)

    market = {
        'AUSTRALIA':'AU',
        'THAILAND':'TH',
        'SINGAPORE':'SG',
        'INDONESIA':'ID',
        'MALAYSIA':'MY',
        'VIETNAM':'VN',
        'PHILIPPINES':'PH',
        'NEW ZEALAND':'NZ'
    }
    df['Market Area'] = df['Market Area'].replace(market)

    #format str cols
    for col in ['Campaign Start', 'Campaign End', 'Executed Date', 'Campaign Id', 'Program ID', 'Target Group ID']:
        df[col] = df[col].astype(str).str.removesuffix('.0')
    
    #format int cols
    for col in ['Targeted Sent', 'Delivery Success', 'Opened/Displayed', 'Clicked', 'Unsubscription', 'Delivery Failed']:
        df[col] = df[col].str.replace(',', '').fillna(0).astype(int)
    
    df['HYBRIS_ID'] = '0000' + df['Campaign Id']
    df['date'] = pd.to_datetime(df['Executed Date'], format='%Y%m%d').dt.date
    df['Email Title'] = df['Email Title'].str.replace('\|.*\|', 'First Name', regex=True)
    df['Email Title'] = df['Email Title'].str.replace(r'\[QC\]\s*', '', regex=True) \
                                     .str.replace(r'QC\]\s*', '', regex=True)

    #identify and remove test campaigns
    df['Test'] = np.where(df['Campaign'].str.lower().str.contains('|'.join(['test', 'dry'])), 1, 0)
    df = df[df['Test'] == 0]

    ppc = [
        'elife',
        'pplc',
        'ppx',
        'early',
        'onboard'
    ]

    # Post-purchase campaigns have different tagging logic
    df['PPC'] = np.where(df['Campaign'].str.lower().str.contains('|'.join(ppc)) | (df['Marketing event'] == 'On Boarding') | (df['Marketing type'] == 'OnBoarding'), 1, 0)
    df['Marketing event'] = np.where(df['PPC'] == 1, 'On Boarding', df['Marketing event'])

    # Safety protocol to prevent duplication in source data
    df = df.drop_duplicates(subset=['HYBRIS_ID'], keep='last')

    return df


def model_tag_campaign_name(df, mapper_path):    
    # Combine Campaign name & product for tagging by name
    df['Campaign_Full_Info'] = df['Campaign'].astype(str) + df['Product'].astype(str)

    # Read the campaign name model mapping file
    product_map = pd.read_excel(mapper_path, na_filter=False)
    product_map = product_map.set_index('Pattern')

    pattern_list = product_map.index.to_list()
    pattern_list.reverse() #prioritize patterns at the end of the list

    # Create a new column called `Model` that extracts the product name from the campaign name
    def extract_model_name(c_name):
        substring = c_name[2:7].lower()
        for pattern in pattern_list:
            if pattern.lower() == substring:
                return product_map.loc[pattern, 'Model']

    df['Model'] = df['Campaign_Full_Info'].apply(extract_model_name)
    
    df = df.drop(columns=['Campaign_Full_Info'])

    return df


def model_tag_single_text_file(text, product_map, debug=False):
    text_content = text.lower()

    matched_models = {} #for storing all matched models and its position in the text file
    for pattern in product_map.index:
        model = product_map.loc[pattern, 'Model']
        pattern = pattern.lower()

        if re.search(pattern, text_content):
            if model not in matched_models:
                #get the position in edm creative
                matched_models[model] = text_content.index(pattern.split("[")[0])
            
            if debug:
                print(pattern, model)
    
    return matched_models


def model_tag_extracted_text(df, mapper_path, text_folder_path, last_executed_date, service_cred_path):
    import utils.blob_downloader as bdl
    from datetime import datetime, timedelta

    # Subtract 5 days from last_executed_date to ensure no missing campaign
    last_executed_date = datetime.strftime(datetime.strptime(last_executed_date, '%Y-%m-%d') - timedelta(days=5), '%Y%m%d')

    # Read the product mapping file
    product_map = pd.read_excel(mapper_path, na_filter=False)
    product_map.set_index('Pattern', inplace=True)

    # Get a list of all text files to download from GCS
    df['gcs_text_path'] = df['Market Area'].str.lower() + "/" + df['HYBRIS_ID'] + ".txt"
    gcs_text_files = df[(df['Channel'] == 'EMAIL') & (df['Executed Date'] > last_executed_date)]['gcs_text_path'].to_list()   

    df_tag = {
        'HYBRIS_ID':[],
        'Main_Model':[],
        'All_Models':[]
    }

    # Download and tag each text file
    bucket = bdl.get_bucket(service_cred_path=service_cred_path, bucket_name='creative-edm-text')

    for text_file in gcs_text_files:
        text_download_path = f'{text_folder_path}/' + text_file.split('/')[1]
        if not os.path.exists(text_download_path):
            try:
                bdl.download_text_file(bucket=bucket, text_file=text_file, text_download_path=text_download_path)
            except:
                print(f"Campaign {text_file.split('.')[0]} not found in creative-edm-text!")
                os.remove(text_download_path) #client created an empty text file even if file not found
                continue
        
        with open(text_download_path, 'r', encoding='utf-8') as f:
            text = f.read()

        matched_models = model_tag_single_text_file(text, product_map)
        if len(matched_models) > 0:
            matched_models = [k for k, v in sorted(matched_models.items(), key=lambda item: item[1])] #sort by model position in text and convert to list of models
            df_tag['HYBRIS_ID'].append(text_file.split('.')[0].split('/')[1])
            df_tag['Main_Model'].append(matched_models[0]) #first captured model is the main model
            df_tag['All_Models'].append(','.join(matched_models)) #store all captured models as a comma separated string

    df_tag = pd.DataFrame(df_tag)

    # Remap with previous df
    df = df.merge(df_tag, on='HYBRIS_ID', how='left')

    # Remove first method (campaign name) model from all models for PPC campaigns
    def ppc_remove_model(model, all_models, ppc):
        if pd.isna(model):
            return all_models
        elif ppc == 1:
            try:
                all_models = all_models.split(',')
            except:
                return np.nan
            try:
                all_models.remove(model)
            except:
                pass
            if len(all_models) > 0:
                return ','.join(all_models)
            else:
                return np.nan
        else:
            return all_models
    df['All_Models'] = df.apply(lambda x: ppc_remove_model(x['Model'], x['All_Models'], x['PPC']), axis=1)

    # Prioritize first method (campaign name) then second method (text extraction), except PPC campaigns.
    def main_model(model, main_model, all_models, ppc):
        if pd.isna(model):
            return main_model
        elif ppc == 1:
            try:
                all_models = all_models.split(',')
            except:
                return np.nan
            return all_models[0]
        else:
            return model
    df['Model'] = df.apply(lambda x: main_model(x['Model'], x['Main_Model'], x['All_Models'], x['PPC']), axis=1)

    # Add model from method 1 to all models if it does not exist already
    def append_models(all_models, model):
        if pd.isna(all_models):
            return model
        elif model in all_models:
            return all_models
        else:
            return model + ',' + all_models
    df['All_Models'] = df.apply(lambda x: append_models(x['All_Models'], x['Model']), axis=1)

    return df


def export_results(df, out_path_data_hub, out_path_data, out_path_preview, out_path_data_all_models):
    # Cleanup columns for data-hub upload file
    data_hub_cols = [
        'Campaign Start',
        'Campaign End',
        'Executed Date',
        'Campaign Id',
        'Campaign',
        'Campaign Category',
        'Category',
        'Market Area',
        'Channel',
        'Provider',
        'Targeted Sent',
        'Delivery Success',
        'Opened/Displayed',
        'Clicked',
        'Unsubscription',
        'Model',
        'HYBRIS_ID',
        'date'
    ]

    data_hub = df[data_hub_cols]
    data_hub.columns = [col.replace(' ', '_').replace('/', '_') for col in data_hub_cols]

    # Cleanup columns for data-digitas upload file
    data_drop_cols = [
        'Campaign Name(2)',
        'SMS Message Part',
        'Push execute',
        'Test',
        'PPC',
        'Main_Model',
        'All_Models',
        'gcs_text_path'
    ]

    data = df.drop(columns=data_drop_cols)
    data.columns = [col.replace(' ', '_').replace('/', '_') for col in data.columns]
    data['source'] = 'bo_report'
    data['Clicked_Non_Unique'] = 0 #create dummy non-unique click count column. This col is populated later in BQ using campaign_performance table

    # Export to parquet files
    data_hub.to_parquet(out_path_data_hub, index=False)
    data.to_parquet(out_path_data, index=False)
    
    # Export preview file for optional manual revision
    df.to_excel(out_path_preview, index=False)

    # Export all models parquet file
    data_all_models = df[['HYBRIS_ID', 'All_Models']]
    data_all_models = data_all_models.assign(All_Models=df['All_Models'].str.split(',')).explode('All_Models') #https://stackoverflow.com/questions/12680754/split-explode-pandas-dataframe-string-entry-to-separate-rows
    data_all_models.columns = ['HYBRIS_ID', 'Model']
    data_all_models = data_all_models[(data_all_models['Model'].notna()) & (data_all_models['Model'] != "")]
    data_all_models.to_parquet(out_path_data_all_models, index=False)


def manual_revision(preview, out_path_data_hub, out_path_data, out_path_data_all_models):
    df_preview = pd.read_excel(preview, dtype=str)
    data_preview = df_preview[['HYBRIS_ID', 'Model']].sort_values('HYBRIS_ID')

    data = pd.read_parquet(out_path_data)
    data = data.sort_values('HYBRIS_ID')
    data_hub = pd.read_parquet(out_path_data_hub)
    data_hub = data_hub.sort_values('HYBRIS_ID')

    data['Model'] = data_preview['Model']
    data_hub['Model'] = data_preview['Model']

    data.to_parquet(out_path_data, index=False)
    data_hub.to_parquet(out_path_data_hub, index=False)

    data_all_models = df_preview[['HYBRIS_ID', 'All_Models']]
    data_all_models = data_all_models.assign(All_Models=df_preview['All_Models'].str.split(',')).explode('All_Models')
    data_all_models.columns = ['HYBRIS_ID', 'Model']
    data_all_models = data_all_models[data_all_models['Model'].notna()]
    data_all_models.to_parquet(out_path_data_all_models, index=False)

    return


def update_bq_table(tbl_date, out_path_data_hub, out_path_data, out_path_data_all_models, client_secret_path, run_list=[1,2,3,4,5]):
    from google.cloud import bigquery
    from google_auth_oauthlib import flow

    appflow = flow.InstalledAppFlow.from_client_secrets_file(
        client_secret_path, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    appflow.run_local_server()
    credentials = appflow.credentials

    # Confirmation for deleting snapshot campaigns table
    if 4 in run_list:
        confirmation = input(f"This operation will DELETE table xxx.gcdm_snapshot.campaigns_{tbl_date} if exist. Proceed? (y/n)")
        if confirmation == 'y':
            pass
        else:
            return

    # Upload campaigns_temp in data-hub
    client_datahub = bigquery.Client(project = 'xxx', credentials=credentials)
    tbl_campaigns_datahub = client_datahub.dataset('gcdm').table('campaigns_temp')

    job_config = bigquery.LoadJobConfig(
         autodetect=True,
         source_format=bigquery.SourceFormat.PARQUET,
    )
    with open(out_path_data_hub, 'rb') as source_file:
        if 3 in run_list:
            load_job = client_datahub.load_table_from_file(source_file, tbl_campaigns_datahub, job_config=job_config)
            load_job.result()

    # Upload campaigns_temp and campaign_models_temp in data-digitas
    client = bigquery.Client(project = 'xxx', credentials=credentials)
    tbl_campaigns = client.dataset('gcdm').table('campaigns_temp')
    tbl_campaign_models = client.dataset('gcdm').table('campaign_models_temp')

    job_config = bigquery.LoadJobConfig(
         autodetect=True,
         source_format=bigquery.SourceFormat.PARQUET,
    )
    with open(out_path_data, 'rb') as source_file:
        if 1 in run_list:
            load_job = client.load_table_from_file(source_file, tbl_campaigns, job_config=job_config)
            load_job.result()
    with open(out_path_data_all_models, 'rb') as source_file:
        if 2 in run_list:
            load_job = client.load_table_from_file(source_file, tbl_campaign_models, job_config=job_config)
            load_job.result()

    QUERY1 = (
        f"""
        
        DELETE `xxx.gcdm.campaigns` 
        WHERE source = 'temporary';

        MERGE `xxx.gcdm.campaigns` T
        USING `xxx.gcdm.campaigns_temp` S
        ON T.HYBRIS_ID = S.HYBRIS_ID
        WHEN MATCHED THEN
        UPDATE SET date = S.date, Executed_Date = S.Executed_Date, Campaign_End = S.Campaign_End, Email_Title = S.Email_Title
        WHEN NOT MATCHED THEN
        INSERT ROW
        ;

        DROP TABLE IF EXISTS `xxx.gcdm.campaigns_temp`
        ;

        """
    )

    QUERY2 = (
        f"""

        MERGE `xxx.gcdm.campaign_models` T
        USING `xxx.gcdm.campaign_models_temp` S
        ON T.HYBRIS_ID = S.HYBRIS_ID
        WHEN NOT MATCHED THEN
        INSERT ROW
        ;

        DROP TABLE IF EXISTS `xxx.gcdm.campaign_models_temp`
        ;

        """
    )

    QUERY3 = (
        f"""

        CREATE OR REPLACE TABLE `xxx.gcdm.campaigns_temp` AS
        (SELECT DISTINCT *
        FROM `xxx.gcdm.campaigns_temp`)

        ;

        MERGE `xxx.gcdm.campaigns` T
        USING `xxx.gcdm.campaigns_temp` S
        ON T.HYBRIS_ID = S.HYBRIS_ID
        WHEN MATCHED THEN
        UPDATE SET date = S.date, Executed_Date = S.Executed_Date, Campaign_End = S.Campaign_End, Targeted_Sent = S.Targeted_Sent, Delivered = S.Delivery_Success, Opened_Displayed = S.Opened_Displayed, Clicked = S.Clicked, Unsubscription = S.Unsubscription
        WHEN NOT MATCHED THEN
        INSERT ROW
        ;

        DROP TABLE IF EXISTS `xxx.gcdm.campaigns_temp`
        ;

        """
    )

    QUERY4 = (
        f"""

        DROP TABLE IF EXISTS `xxx.gcdm_snapshot.campaigns_{tbl_date}`
        ;

        CREATE TABLE `xxx.gcdm_snapshot.campaigns_{tbl_date}`
        COPY `xxx.gcdm.campaigns`
        ;

        """
    )

    QUERY5 = (
        f""" 

        MERGE `xxx.gcdm.campaign_performance` T
        USING (
        WITH
        `unique_customers` AS (
        SELECT
            c.HYBRIS_ID,
            c.Channel,
            count(distinct CASE WHEN ia_type IN ('EMAIL_OUTBOUND', 'EMAIL_BOUNCE_SOFT', 'EMAIL_BOUNCE_HARD', 'PUSH_OUTBOUND', 'PUSH_DELIVERED') THEN gcdm_customer_id END) AS Targeted_Sent,
            count(distinct CASE WHEN ia_type IN ('EMAIL_OUTBOUND', 'PUSH_DELIVERED') THEN gcdm_customer_id END) AS Delivery_Success,
            count(distinct CASE WHEN ia_type IN ('EMAIL_OPENED', 'PUSH_DISPLAYED') THEN gcdm_customer_id END) AS Opened_Displayed,
            count(distinct CASE WHEN ia_type IN ('CLICK_THROUGH', 'PUSH_CLICK') THEN gcdm_customer_id END) AS Clicked,
            count(distinct CASE WHEN ia_type IN ('EMAIL_BOUNCE_SOFT', 'EMAIL_BOUNCE_HARD') THEN gcdm_customer_id END) AS Delivery_Failed,
            count(distinct CASE WHEN ia_type = 'PUSH_OUTBOUND' THEN gcdm_customer_id END) AS Push_Outbound,
            count(CASE WHEN ia_type IN ('CLICK_THROUGH', 'PUSH_CLICK') THEN gcdm_customer_id END) AS Clicked_Non_Unique
        FROM
            `xxx.gcdm.campaigns` c 
            JOIN (SELECT gcdm_customer_id, campaign_id, ia_type, ia_date FROM `xxx.gcdm.interactions` GROUP BY 1,2,3,4) ia ON c.HYBRIS_ID = ia.campaign_id
        WHERE
            ia.ia_type IN ('EMAIL_OUTBOUND', 'PUSH_DELIVERED', 'EMAIL_OPENED', 'PUSH_DISPLAYED', 'CLICK_THROUGH', 'PUSH_CLICK', 'EMAIL_BOUNCE_SOFT', 'EMAIL_BOUNCE_HARD', 'PUSH_OUTBOUND')
            AND c.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
        GROUP BY 1,2

        )

        SELECT
            HYBRIS_ID,
            Targeted_Sent,
            Delivery_Success,
            Opened_Displayed,
            Clicked,
            Clicked_Non_Unique,
            CASE
            WHEN Channel = 'EMAIL' THEN Delivery_Failed --for EMAIL
            ELSE Targeted_Sent - Delivery_Success
            END AS Delivery_Failed,

            CASE
            WHEN Channel = 'EMAIL' THEN NULL
            WHEN Targeted_Sent = Push_Outbound THEN 'PUSH_OUTBOUND'
            WHEN Targeted_Sent > Push_Outbound THEN 'PUSH_OUTBOUND + PUSH_DELIVERED'
            END AS push_targeted_sent_event
        FROM
            unique_customers
        ) S
        ON T.HYBRIS_ID = S.HYBRIS_ID
        WHEN MATCHED THEN
        UPDATE SET Delivery_Success = S.Delivery_Success, Opened_Displayed = S.Opened_Displayed, Clicked = S.Clicked, Clicked_Non_Unique = S.Clicked_Non_Unique, Delivery_Failed = S.Delivery_Failed, Targeted_Sent = S.Targeted_Sent, push_targeted_sent_event = S.push_targeted_sent_event
        WHEN NOT MATCHED THEN
        INSERT ROW
        ;

        MERGE `xxx.gcdm.campaigns` T
        USING `xxx.gcdm.campaign_performance` S
        ON T.HYBRIS_ID = S.HYBRIS_ID
        WHEN MATCHED THEN
        UPDATE SET Delivery_Success = S.Delivery_Success, Opened_Displayed = S.Opened_Displayed, Clicked = S.Clicked, Clicked_Non_Unique = S.Clicked_Non_Unique, Delivery_Failed = S.Delivery_Failed, Targeted_Sent = S.Targeted_Sent, source = 'interactions'
        ;


        """
    )

    query_list = [QUERY1, QUERY2, QUERY3, QUERY4, QUERY5]
    query_message = {
        QUERY1: 'Succesfully updated xxx.gcdm.campaigns',
        QUERY2: 'Succesfully updated xxx.gcdm.campaign_models',
        QUERY3: 'Succesfully updated xxx.gcdm.campaigns',
        QUERY4: 'Succesfully updated xxx.gcdm_snapshots.campaigns',
        QUERY5: 'Succesfully updated xxx.gcdm.campaign_performance'
    }

    selected_query_list = [query_list[i-1] for i in run_list]
    selected_query_message = {query: query_message.get(query, "") for query in selected_query_list}

    for query, message in selected_query_message.items():
        job = client.query(query)
        res = job.result()  # Waits for query to finish
        if job.errors:
            print('Query failed to execute!')
            print(job.errors)
            continue
        print(message)
