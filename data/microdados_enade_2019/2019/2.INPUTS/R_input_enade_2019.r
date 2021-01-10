##################################################################################
# MEC/INEP/DAES (Diretoria de Avaliação da Educação Superior) #
# Coordenação Geral de Controle de Qualidade da Educação Superior #
#--------------------------------------------------------------------------------#
# Programa: #
# R_input_enade_2019.R (Pasta "INPUTS") #
#--------------------------------------------------------------------------------#
# Descrição: #
# Programa para Leitura dos Microdados do enade 2019 #
# #
#********************************************************************************#
# Obs: Para executar este programa é necessário salvar o arquivo #
# "microdados_enade_2019.txt" (Pasta "DADOS") no diretório "C:\" do computador. #
# #
#********************************************************************************#


microdados_enade <- read.table("C:/microdados_enade_2019.txt",header = TRUE, sep=";", dec = ",", colClasses=c(DS_VT_ACE_OFG="character",DS_VT_ACE_OCE="character"))

