import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


default_args = {
  'owner': 'airflow',
}

@dag(
  default_args=default_args,
  description="Desafio",
  schedule_interval=None,
  start_date=days_ago(2)
)

def desafio():

  @task()
  def get_raw_data():
    path = '/opt/airflow/data/inputs/'
    return path

  @task()
  def read_data_1(path):
    print('entrou')
    companies = pd.read_csv(path + 'companies.tsv', sep=",")
    return companies

  @task()
  def read_data_2(path):
    sectors = pd.read_csv(path + "sectors.tsv", sep="\t")
    return sectors

  @task()
  def read_data_3(path):
    deals = pd.read_csv(path + "deals.tsv", sep="\t")
    return deals 

  @task()
  def read_data_4(path):
    contacts = pd.read_csv(path + "contacts.tsv", sep="\t")
    return contacts  

  @task()
  def merge_d1(companies,sectors):
    comp_sector = pd.merge(companies[['companiesId','companiesName','sectorKey']],
                          sectors[['sectorKey','sector']],
                          on='sectorKey')
    return comp_sector

  @task()
  def merge_d2(comp_sector,deals):
    comp_deals = pd.merge(comp_sector[['companiesId','companiesName','sectorKey','sector']],
                            deals[['dealsId','dealsDateCreated','dealsPrice','contactsId','companiesId']],
                            on='companiesId')
    return comp_deals

  @task()
  def merge_d3(comp_deals,contacts):
    contacts = contacts.rename({' contactsId': 'contactsId'}, axis=1)
    deals_full = pd.merge(comp_deals[['companiesId','companiesName','sectorKey','sector','dealsId','dealsDateCreated',                                      'dealsPrice','contactsId']],
                            contacts[['contactsId','contactsName']],
                            on='contactsId', how='left')
    return deals_full

  @task()
  def transfom_data(deals_full):
    deals_full['dealsDateCreated'] = pd.to_datetime(deals_full['dealsDateCreated'], format='%d-%m-Y',infer_datetime_format=True)
    return deals_full

  @task()
  def separate_year(deals_full):
    deals_2017 = deals_full.loc[(deals_full['dealsDateCreated'].dt.year == 2017)]
    deals_2018 = deals_full.loc[(deals_full['dealsDateCreated'].dt.year == 2018)]
    deals_2019 = deals_full.loc[(deals_full['dealsDateCreated'].dt.year == 2019)]
    return (deals_2017,deals_2018,deals_2019)

  @task()
  def consult_2017(deals_2017):  
    vendas_contacts_2017 = deals_2017.groupby(deals_2017['contactsName'])['dealsPrice'].sum()
    vendas_mes_2017 = deals_2017.groupby(deals_2017['dealsDateCreated'].dt.month)['dealsPrice'].sum()
    vendas_sectors_2017 = deals_2017.groupby(deals_2017['sector'])['dealsPrice'].sum()
    vendas_sectors_o_2017 = pd.DataFrame(vendas_sectors_2017)
    vendas_sectors_o_2017.sort_values('dealsPrice', ascending=False)
    return consult_2017

  @task()
  def consult_2018(deals_2018): 
    vendas_contacts_2018 = deals_2018.groupby(deals_2018['contactsName'])['dealsPrice'].sum()
    vendas_mes_2018 = deals_2018.groupby(deals_2018['dealsDateCreated'].dt.month)['dealsPrice'].sum()
    vendas_sectors_2018 = deals_2018.groupby(deals_2018['sector'])['dealsPrice'].sum()
    vendas_sectors_o_2018 = pd.DataFrame(vendas_sectors_2018)
    vendas_sectors_o_2018.sort_values('dealsPrice', ascending=False)
    return consult_2018

  @task()
  def consult_2019(deals_2019):  
    vendas_contacts_2019 = deals_2019.groupby(deals_2019['contactsName'])['dealsPrice'].sum()
    vendas_mes_2019 = deals_2019.groupby(deals_2019['dealsDateCreated'].dt.month)['dealsPrice'].sum()
    vendas_sectors_2019 = deals_2019.groupby(deals_2019['sector'])['dealsPrice'].sum()
    vendas_sectors_o_2019 = pd.DataFrame(vendas_sectors_2019)
    vendas_sectors_o_2019.sort_values('dealsPrice', ascending=False)
    return consult_2019




  path = get_raw_data()
  read_1 = read_data_1(path)
  read_2 = read_data_2(path)
  read_3 = read_data_3(path)
  read_4 = read_data_4(path)
  m_d1 = merge_d1(read_1,read_2)
  m_d2 = merge_d2(m_d1,read_3)
  m_d3 = merge_d3(m_d2,read_4)
  t_date = transfom_data(m_d3)


eng_desafio = desafio()