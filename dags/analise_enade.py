import pandas as pd
import zipfile
import urllib.request

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

#Constantes
data_path = '/opt/airflow/data/microdados_enade_2019/2019/3.DADOS/'
arquivo = data_path + 'microdados_enade_2019.txt'

default_args = {
  'owner': 'airflow',
}

@dag(
  default_args=default_args,
  description="Fazer analise dos dados do enade 2019",
  schedule_interval=None,
  start_date=days_ago(2)
)

def analise_enade():

  @task()
  def get_data():
    url = 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip'

    urllib.request.urlretrieve(url, '/opt/airflow/data/microdados_enade_2019.zip')

    var = 'sucesso'

    return var

  @task()
  def unzip_file(multiple_outputs=True):
    with zipfile.ZipFile('/opt/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
      zipped.extractall('/opt/airflow/data/')

    var = 'sucesso'

    return var

  @task()
  def aplicar_filtros(multiple_outputs=True):
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
          'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(arquivo, sep=';', decimal=',', usecols=cols)
    enade = enade.loc[
      (enade.NU_IDADE > 20) & 
      (enade.NU_IDADE < 40) & 
      (enade.NT_GER > 0)
    ]        
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False) 
    var = 'sucesso'

    return var  

  @task()
  def const_idade_centralizada(multiple_outputs=True):
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + "idadecent.csv", index=False)

    var = 'sucesso'

    return var  

  @task()
  def const_idade_cent_quad(multiple_outputs=True):
    idadecent = pd.read_csv(data_path + "idadecent.csv")
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + "idadequadrado.csv", index=False)

    var = 'sucesso'

    return var 

  @task()
  def const_est_civil(multiple_outputs=True):
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
      'A': 'Solteiro',
      'B': 'Casado',
      'C': 'Separado',
      'D': 'Viúvo',
      'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)

    var = 'sucesso'

    return var 

  @task()
  def const_cor(multiple_outputs=True):
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QE_I02.replace({
      'A': 'Branca',
      'B': 'Preta',
      'C': 'Amarela',
      'D': 'Parda',
      'E': 'Indigena',
      'F': "",
      ' ': ""
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)
    
    var = 'sucesso'

    return var 

  @task()
  def const_escopai(multiple_outputs=True):
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I04'])
    filtro['escopai'] = filtro.QE_I04.replace({
      'A': 0,
      'B': 1,
      'C': 2,
      'D': 3,
      'E': 4,
      'F': 5
    })
    filtro[['escopai']].to_csv(data_path + 'escopai.csv', index=False)

    var = 'sucesso'

    return var 

  @task()
  def const_escomae(multiple_outputs=True):
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I05'])
    filtro['escomae'] = filtro.QE_I05.replace({
      'A': 0,
      'B': 1,
      'C': 2,
      'D': 3,
      'E': 4,
      'F': 5
    })
    filtro[['escomae']].to_csv(data_path + 'escomae.csv', index=False)

    var = 'sucesso'

    return var 

  @task()
  def const_renda(multiple_outputs=True):
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I08'])
    filtro['renda'] = filtro.QE_I08.replace({
      'A': 0,
      'B': 1,
      'C': 2,
      'D': 3,
      'E': 4,
      'F': 5,
      'G': 6
    })
    filtro[['renda']].to_csv(data_path + 'renda.csv', index=False)

    var = 'sucesso'

    return var 

  @task()  
  def join_data(multiple_outputs=True):
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadequadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')
    escopai = pd.read_csv(data_path + 'escopai.csv')
    escomae = pd.read_csv(data_path + 'escomae.csv')
    renda = pd.read_csv(data_path + 'renda.csv')

    final = pd.concat([
      filtro, idadequadrado, estcivil, cor,
      escopai,escomae,renda
    ], 
    axis=1
    )

    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)


  data = get_data()
  extract = unzip_file(data)
  apl_f = aplicar_filtros(extract)

  c_idade_cent = const_idade_centralizada(apl_f)
  c_idade_cent_s = const_idade_cent_quad(c_idade_cent)
  c_est_civil = const_est_civil(apl_f)
  c_cor = const_cor(apl_f)
  c_escopai = const_escopai(apl_f)
  c_escomae = const_escomae(apl_f)
  c_renda = const_renda(apl_f)

  join_data(([c_idade_cent_s,c_est_civil,c_cor,
                    c_escopai,c_escomae,c_renda])) 

analise_enade_dag = analise_enade()  