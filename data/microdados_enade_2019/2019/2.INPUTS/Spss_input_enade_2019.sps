* Encoding: windows-1252.
***************************************************************************** 
*MEC/INEP/DAES (Diretoria de Avaliação da Educação Superior) 
*Coordenação Geral de Controle de Qualidade da Educação Superior
*------------------------------------------------------------------------------ 
*Programa: 
*	Spss_input_enade_2019.sps (Pasta "INPUTS")
*------------------------------------------------------------------------------ 
*Descrição: 
*	Programa para Leitura dos Microdados do enade 2019 e para colocar labels
*
****************************************************************************** 
*Obs: Para executar este programa é necessário salvar o arquivo 
*"microdados_enade_2019.txt" (Pasta "DADOS") no diretório "C:\" do computador.
* 
******************************************************************************

GET DATA  /TYPE=TXT
  /FILE="C:\microdados_enade_2019.txt."
  /ENCODING='Locale'
  /DELCASE=LINE
  /DELIMITERS=";"
  /ARRANGEMENT=DELIMITED
  /FIRSTCASE=2
  /IMPORTCASE=ALL
  /VARIABLES=
  NU_ANO F4.0
  CO_IES F5.0
  CO_CATEGAD F5.0
  CO_ORGACAD F5.0
  CO_GRUPO F4.0
  CO_CURSO F7.0
  CO_MODALIDADE F1.0
  CO_MUNIC_CURSO F7.0
  CO_UF_CURSO F2.0
  CO_REGIAO_CURSO F1.0
  NU_IDADE F3.0
  TP_SEXO A1
  ANO_FIM_EM F4.0
  ANO_IN_GRAD F4.0
  CO_TURNO_GRADUACAO F1.0
  TP_INSCRICAO_ADM F1.0
  TP_INSCRICAO F1.0
  NU_ITEM_OFG F1.0
  NU_ITEM_OFG_Z F1.0
  NU_ITEM_OFG_X F1.0
  NU_ITEM_OFG_N F1.0
  NU_ITEM_OCE F2.0
  NU_ITEM_OCE_Z F2.0
  NU_ITEM_OCE_X F2.0
  NU_ITEM_OCE_N F2.0
  DS_VT_GAB_OFG_ORIG A8
  DS_VT_GAB_OFG_FIN A8
  DS_VT_GAB_OCE_ORIG A27
  DS_VT_GAB_OCE_FIN A27
  DS_VT_ESC_OFG A8
  DS_VT_ACE_OFG A8
  DS_VT_ESC_OCE A27
  DS_VT_ACE_OCE A27
  TP_PRES F3.0
  TP_PR_GER F3.0
  TP_PR_OB_FG F3.0
  TP_PR_DI_FG F3.0
  TP_PR_OB_CE F3.0
  TP_PR_DI_CE F3.0
  TP_SFG_D1 F3.0
  TP_SFG_D2 F3.0
  TP_SCE_D1 F3.0
  TP_SCE_D2 F3.0
  TP_SCE_D3 F3.0
  NT_GER F4.2
  NT_FG F4.2
  NT_OBJ_FG F4.2
  NT_DIS_FG F4.2
  NT_FG_D1 F3.2
  NT_FG_D1_PT F3.2
  NT_FG_D1_CT F3.2
  NT_FG_D2 F3.2
  NT_FG_D2_PT F3.2
  NT_FG_D2_CT F3.2
  NT_CE F4.2
  NT_OBJ_CE F4.2
  NT_DIS_CE F4.2
  NT_CE_D1 F3.2
  NT_CE_D2 F3.2
  NT_CE_D3 F3.2
  CO_RS_I1 A1
  CO_RS_I2 A1
  CO_RS_I3 A1
  CO_RS_I4 A1
  CO_RS_I5 A1
  CO_RS_I6 A1
  CO_RS_I7 A1
  CO_RS_I8 A1
  CO_RS_I9 A1
  QE_I01 A1
  QE_I02 A1
  QE_I03 A1
  QE_I04 A1
  QE_I05 A1
  QE_I06 A1
  QE_I07 A1
  QE_I08 A1
  QE_I09 A1
  QE_I10 A1
  QE_I11 A1
  QE_I12 A1
  QE_I13 A1
  QE_I14 A1
  QE_I15 A1
  QE_I16 F2.0
  QE_I17 A1
  QE_I18 A1
  QE_I19 A1
  QE_I20 A1
  QE_I21 A1
  QE_I22 A1
  QE_I23 A1
  QE_I24 A1
  QE_I25 A1
  QE_I26 A1
  QE_I27 F1.0
  QE_I28 F1.0
  QE_I29 F1.0
  QE_I30 F1.0
  QE_I31 F1.0
  QE_I32 F1.0
  QE_I33 F1.0
  QE_I34 F1.0
  QE_I35 F1.0
  QE_I36 F1.0
  QE_I37 F1.0
  QE_I38 F1.0
  QE_I39 F1.0
  QE_I40 F1.0
  QE_I41 F1.0
  QE_I42 F1.0
  QE_I43 F1.0
  QE_I44 F1.0
  QE_I45 F1.0
  QE_I46 F1.0
  QE_I47 F1.0
  QE_I48 F1.0
  QE_I49 F1.0
  QE_I50 F1.0
  QE_I51 F1.0
  QE_I52 F1.0
  QE_I53 F1.0
  QE_I54 F1.0
  QE_I55 F1.0
  QE_I56 F1.0
  QE_I57 F1.0
  QE_I58 F1.0
  QE_I59 F1.0
  QE_I60 F1.0
  QE_I61 F1.0
  QE_I62 F1.0
  QE_I63 F1.0
  QE_I64 F1.0
  QE_I65 F1.0
  QE_I66 F1.0
  QE_I67 F1.0
  QE_I68 F1.0.
CACHE.
EXECUTE.
DATASET NAME ConjuntodeDados1 WINDOW=FRONT.




VARIABLE LABELS NU_ANO 'Ano de realização do exame'.
VARIABLE LABELS CO_IES 'Código da IES (e-MEC)'.
VARIABLE LABELS CO_CATEGAD 'Código da categoria administrativa da IES'.
VARIABLE LABELS CO_ORGACAD 'Código da organização acadêmica da IES'.
VARIABLE LABELS CO_GRUPO 'Código da Área de enquadramento do curso no Enade'.
VARIABLE LABELS CO_CURSO 'Código do curso no Enade'.
VARIABLE LABELS CO_MODALIDADE 'Código da Modalidade de Ensino'.
VARIABLE LABELS CO_MUNIC_CURSO 'Código do município de funcionamento do curso'.
VARIABLE LABELS CO_UF_CURSO 'Código da UF de funcionamento do curso'.
VARIABLE LABELS CO_REGIAO_CURSO 'Código da região de funcionamento do curso'.
VARIABLE LABELS NU_IDADE 'Idade do inscrito em 24/11/2019'.
VARIABLE LABELS TP_SEXO 'Sexo'.
VARIABLE LABELS ANO_FIM_EM 'Ano de conclusão do Ensino Médio'.
VARIABLE LABELS ANO_IN_GRAD 'Ano de início da graduação'.
VARIABLE LABELS CO_TURNO_GRADUACAO 'Código do turno de graduação'.
VARIABLE LABELS TP_INSCRICAO_ADM 'Forma pela qual foi realizada a inscrição'.
VARIABLE LABELS TP_INSCRICAO 'Tipo de inscrição'.
VARIABLE LABELS NU_ITEM_OFG 'Número de itens da parte objetiva da Formação Geral'.
VARIABLE LABELS NU_ITEM_OFG_Z 'Número de itens da parte objetiva da Formação Geral que foram excluídos devido a anulação'.
VARIABLE LABELS NU_ITEM_OFG_X 'Número de itens da parte objetiva da Formação Geral que foram excluídos devido ao coeficiente ponto-bisserial menor que 0,20'.
VARIABLE LABELS NU_ITEM_OFG_N 'Número de itens da parte objetiva da Formação Geral que não se aplicam ao grupo de curso'.
VARIABLE LABELS NU_ITEM_OCE 'Número de itens da parte objetiva de Componente Específico'.
VARIABLE LABELS NU_ITEM_OCE_Z 'Número de itens da parte objetiva de Componente Específico que foram excluídos devido a anulação'.
VARIABLE LABELS NU_ITEM_OCE_X 'Número de itens da parte objetiva de Componente Específico que foram excluídos devido ao coeficiente ponto-bisserial menor que 0,20'.
VARIABLE LABELS NU_ITEM_OCE_N 'Número de itens da parte objetiva de Componente Específico que não se aplicam ao grupo de curso'.
VARIABLE LABELS DS_VT_GAB_OFG_ORIG 'Vetor que representa o gabarito original de Formação Geral'.
VARIABLE LABELS DS_VT_GAB_OFG_FIN 'Vetor que representa o gabarito final de Formação Geral'.
VARIABLE LABELS DS_VT_GAB_OCE_ORIG 'Vetor que representa o gabarito original de Componente Específico'.
VARIABLE LABELS DS_VT_GAB_OCE_FIN 'Vetor que representa o gabarito final de Componente Específico'.
VARIABLE LABELS DS_VT_ESC_OFG 'Vetor que representa a escolha de resposta da parte objetiva da Formação Geral'.
VARIABLE LABELS DS_VT_ACE_OFG 'Vetor que representa os acertos da parte objetiva na Formação Geral'.
VARIABLE LABELS DS_VT_ESC_OCE 'Vetor que representa a escolha de resposta da parte objetiva do Componente Específico'.
VARIABLE LABELS DS_VT_ACE_OCE 'Vetor que representa os acertos da parte objetiva do Componente Específico'.
VARIABLE LABELS TP_PRES 'Tipo de presença no Enade'.
VARIABLE LABELS TP_PR_GER 'Tipo de presença na prova'.
VARIABLE LABELS TP_PR_OB_FG 'Tipo de presença na parte objetiva na formação geral'.
VARIABLE LABELS TP_PR_DI_FG 'Tipo de presença na parte discursiva na formação geral'.
VARIABLE LABELS TP_PR_OB_CE 'Tipo de presença na parte objetiva no componente específico'.
VARIABLE LABELS TP_PR_DI_CE 'Tipo de presença na parte discursiva no componente específico'.
VARIABLE LABELS TP_SFG_D1 'Tipo de situação da questão 1 da parte discursiva da formação geral'.
VARIABLE LABELS TP_SFG_D2 'Tipo de situação da questão 2 da parte discursiva da formação geral'.
VARIABLE LABELS TP_SCE_D1 'Tipo de situação da questão 1 da parte discursiva do componente específico'.
VARIABLE LABELS TP_SCE_D2 'Tipo de situação da questão 2 da parte discursiva do componente específico'.
VARIABLE LABELS TP_SCE_D3 'Tipo de situação da questão 3 da parte discursiva do componente específico'.
VARIABLE LABELS NT_GER 'Nota bruta da prova - Média ponderada da formação geral (25%) e componente específico (75%). (valor de 0 a 100)'.
VARIABLE LABELS NT_FG 'Nota bruta na formação geral - Média ponderada da parte objetiva (60%) e discursiva (40%) na formação geral. (valor de 0 a 100)'.
VARIABLE LABELS NT_OBJ_FG 'Nota bruta na parte objetiva da formação geral. (valor de 0 a 100)'.
VARIABLE LABELS NT_DIS_FG 'Nota bruta na parte discursiva da formação geral. (valor de 0 a 100)'.
VARIABLE LABELS NT_FG_D1 'Nota da questão 1 da parte discursiva da formação geral - Média ponderada da parte de Língua Portuguesa (20%) e Conteúdo (80%) da Questão 1 da parte discursiva. (valor de 0 a 100)'.
VARIABLE LABELS NT_FG_D1_PT 'Nota de Língua Portuguesa da questão 1 da parte discursiva da formação geral. (valor de 0 a 100)'.
VARIABLE LABELS NT_FG_D1_CT 'Nota de Conteúdo da questão 1 da parte discursiva da formação geral. (valor de 0 a 100)'.
VARIABLE LABELS NT_FG_D2 'Nota da questão 2 da parte discursiva na formação geral - Média ponderada da parte de Língua Portuguesa (20%) e Conteúdo (80%) da Questão 2 da parte discursiva. (valor de 0 a 100)'.
VARIABLE LABELS NT_FG_D2_PT 'Nota de Língua Portuguesa da questão 2 da parte discursiva da formação geral. (valor de 0 a 100)'.
VARIABLE LABELS NT_FG_D2_CT 'Nota de Conteúdo da questão 2 da parte discursiva da formação geral. (valor de 0 a 100)'.
VARIABLE LABELS NT_CE 'Nota bruta no componente específico - Média ponderada da parte objetiva (85%) e discursiva (15%) no componente específico. (valor de 0 a 100)'.
VARIABLE LABELS NT_OBJ_CE 'Nota bruta na parte objetiva do componente específico. (valor de 0 a 100)'.
VARIABLE LABELS NT_DIS_CE 'Nota bruta na parte discursiva do componente específico. (valor de 0 a 100)'.
VARIABLE LABELS NT_CE_D1 'Nota da questão 1 da parte discursiva do componente específico. (valor de 0 a 100)'.
VARIABLE LABELS NT_CE_D2 'Nota da questão 2 da parte discursiva do componente específico. (valor de 0 a 100)'.
VARIABLE LABELS NT_CE_D3 'Nota da questão 3 da parte discursiva do componente específico. (valor de 0 a 100)'.
VARIABLE LABELS CO_RS_I1 '1 - Qual o grau de dificuldade desta prova na parte de Formação Geral?'.
VARIABLE LABELS CO_RS_I2 '2 - Qual o grau de dificuldade desta prova na parte do Componente Específico?'.
VARIABLE LABELS CO_RS_I3 '3 - Considerando a extensão da prova, em relação ao tempo total, você considera que a prova foi:'.
VARIABLE LABELS CO_RS_I4 '4 - Os enunciados das questões da prova na parte de Formação Geral estavam claros e objetivos?'.
VARIABLE LABELS CO_RS_I5 '5 - Os enunciados das questões na parte do Componente Específico estavam claros e objetivos?'.
VARIABLE LABELS CO_RS_I6 '6 - As informações/instruções fornecidas para a resolução das questões foram suficientes para resolvê-las?'.
VARIABLE LABELS CO_RS_I7 '7 - Você se deparou com alguma dificuldade ao responder à prova. Qual?'.
VARIABLE LABELS CO_RS_I8 '8 - Considerando apenas as questões objetivas da prova, você percebeu que:'.
VARIABLE LABELS CO_RS_I9 '9 - Qual foi o tempo gasto por você para concluir a prova?'.
VARIABLE LABELS QE_I01 'Qual o seu estado civil?'.
VARIABLE LABELS QE_I02 'Qual é a sua cor ou raça?'.
VARIABLE LABELS QE_I03 'Qual a sua nacionalidade?'.
VARIABLE LABELS QE_I04 'Até que etapa de escolarização seu pai concluiu?'.
VARIABLE LABELS QE_I05 'Até que etapa de escolarização sua mãe concluiu?'.
VARIABLE LABELS QE_I06 'Onde e com quem você mora atualmente?'.
VARIABLE LABELS QE_I07  'Quantas pessoas da sua família moram com você? Considere seus pais, irmãos, cônjuge, filhos e outros parentes que moram na mesma casa com você.'.
VARIABLE LABELS QE_I08 'Qual a renda total de sua família, incluindo seus rendimentos?'.
VARIABLE LABELS QE_I09 'Qual alternativa a seguir melhor descreve sua situação financeira (incluindo bolsas)?'.
VARIABLE LABELS QE_I10 'Qual alternativa a seguir melhor descreve sua situação de trabalho (exceto estágio ou bolsas)?'.
VARIABLE LABELS QE_I11 'Que tipo de bolsa de estudos ou financiamento do curso você recebeu para custear todas ou a maior parte das mensalidades? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração.'.
VARIABLE LABELS QE_I12 'Ao longo da sua trajetória acadêmica, você recebeu algum tipo de bolsa de permanência? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração.'.
VARIABLE LABELS QE_I13 'Ao longo da sua trajetória acadêmica, você recebeu algum tipo de bolsa acadêmica? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração.'.
VARIABLE LABELS QE_I14 'Durante o curso de graduação, você participou de programas e ou atividades curriculares no exterior?'.
VARIABLE LABELS QE_I15 'Seu ingresso no curso de graduação se deu por meio de políticas de ação afirmativa ou inclusão social?'.
VARIABLE LABELS QE_I16 'Em que unidade da Federação você concluiu o ensino médio?'.
VARIABLE LABELS QE_I17 'Em que tipo de escola você cursou o ensino médio?'.
VARIABLE LABELS QE_I18 'Qual modalidade de ensino médio você concluiu?'.
VARIABLE LABELS QE_I19 'Quem mais lhe incentivou a cursar a graduação?'.
VARIABLE LABELS QE_I20 'Algum dos grupos abaixo foi determinante para você enfrentar dificuldades durante seu curso superior e conclui-lo?'.
VARIABLE LABELS QE_I21 'Alguém em sua família concluiu um curso superior?'.
VARIABLE LABELS QE_I22 'Excetuando-se os livros indicados na bibliografia do seu curso, quantos livros você leu neste ano?'.
VARIABLE LABELS QE_I23 'Quantas horas por semana, aproximadamente, você dedicou aos estudos, excetuando as horas de aula?'.
VARIABLE LABELS QE_I24 'Você teve oportunidade de aprendizado de idioma estrangeiro na Instituição?'.
VARIABLE LABELS QE_I25 'Qual o principal motivo para você ter escolhido este curso?'.
VARIABLE LABELS QE_I26 'Qual a principal razão para você ter escolhido a sua instituição de educação superior?'.
VARIABLE LABELS QE_I27 'As disciplinas cursadas contribuíram para sua formação integral, como cidadão e profissional.'.
VARIABLE LABELS QE_I28 'Os conteúdos abordados nas disciplinas do curso favoreceram sua atuação em estágios ou em atividades de iniciação profissional.'.
VARIABLE LABELS QE_I29 'As metodologias de ensino utilizadas no curso desafiaram você a aprofundar conhecimentos e desenvolver competências reflexivas e críticas.'.
VARIABLE LABELS QE_I30 'O curso propiciou experiências de aprendizagem inovadoras.'.
VARIABLE LABELS QE_I31 'O curso contribuiu para o desenvolvimento da sua consciência ética para o exercício profissional.'.
VARIABLE LABELS QE_I32 'No curso você teve oportunidade de aprender a trabalhar em equipe.'.
VARIABLE LABELS QE_I33 'O curso possibilitou aumentar sua capacidade de reflexão e argumentação.'.
VARIABLE LABELS QE_I34 'O curso promoveu o desenvolvimento da sua capacidade de pensar criticamente, analisar e refletir sobre soluções para problemas da sociedade.'.
VARIABLE LABELS QE_I35 'O curso contribuiu para você ampliar sua capacidade de comunicação nas formas oral e escrita.'.
VARIABLE LABELS QE_I36 'O curso contribuiu para o desenvolvimento da sua capacidade de aprender e atualizar-se permanentemente.'.
VARIABLE LABELS QE_I37 'As relações professor-aluno ao longo do curso estimularam você a estudar e aprender.'.
VARIABLE LABELS QE_I38 'Os planos de ensino apresentados pelos professores contribuíram para o desenvolvimento das atividades acadêmicas e para seus estudos.'.
VARIABLE LABELS QE_I39 'As referências bibliográficas indicadas pelos professores nos planos de ensino contribuíram para seus estudos e aprendizagens.'.
VARIABLE LABELS QE_I40 'Foram oferecidas oportunidades para os estudantes superarem problemas e dificuldades relacionados ao processo de formação.'.
VARIABLE LABELS QE_I41 'A coordenação do curso esteve disponível para orientação acadêmica dos estudantes.'.
VARIABLE LABELS QE_I42 'O curso exigiu de você organização e dedicação frequente aos estudos.'.
VARIABLE LABELS QE_I43 'Foram oferecidas oportunidades para os estudantes participarem de programas, projetos ou atividades de extensão universitária.'.
VARIABLE LABELS QE_I44 'Foram oferecidas oportunidades para os estudantes participarem de projetos de iniciação científica e de atividades que estimularam a investigação acadêmica.'.
VARIABLE LABELS QE_I45 'O curso ofereceu condições para os estudantes participarem de eventos internos e/ou externos à instituição.'.
VARIABLE LABELS QE_I46 'A instituição ofereceu oportunidades para os estudantes atuarem como representantes em órgãos colegiados.'.
VARIABLE LABELS QE_I47 'O curso favoreceu a articulação do conhecimento teórico com atividades práticas.'.
VARIABLE LABELS QE_I48 'As atividades práticas foram suficientes para relacionar os conteúdos do curso com a prática, contribuindo para sua formação profissional.'.
VARIABLE LABELS QE_I49 'O curso propiciou acesso a conhecimentos atualizados e/ou contemporâneos em sua área de formação.'.
VARIABLE LABELS QE_I50 'O estágio supervisionado proporcionou experiências diversificadas para a sua formação.'.
VARIABLE LABELS QE_I51 'As atividades realizadas durante seu trabalho de conclusão de curso contribuíram para qualificar sua formação profissional.'.
VARIABLE LABELS QE_I52 'Foram oferecidas oportunidades para os estudantes realizarem intercâmbios e/ou estágios no país. '.
VARIABLE LABELS QE_I53 'Foram oferecidas oportunidades para os estudantes realizarem intercâmbios e/ou estágios fora do país.'.
VARIABLE LABELS QE_I54 'Os estudantes participaram de avaliações periódicas do curso (disciplinas, atuação dos professores, infraestrutura).'.
VARIABLE LABELS QE_I55 'As avaliações de aprendizagem realizadas durante o curso foram compatíveis com os conteúdos ou temas trabalhados pelos professores.'.
VARIABLE LABELS QE_I56 'Os professores apresentaram disponibilidade para atender os estudantes fora do horário das aulas.'.
VARIABLE LABELS QE_I57 'Os professores demonstraram domínio dos conteúdos abordados nas disciplinas.'.
VARIABLE LABELS QE_I58 'Os professores utilizaram tecnologias da informação e comunicação (TICs) como estratégia de ensino (projetor, multimídia, laboratório de informática, ambiente virtual de aprendizagem).'.
VARIABLE LABELS QE_I59 'A instituição dispôs de quantidade suficiente de funcionários para o apoio administrativo e acadêmico.'.
VARIABLE LABELS QE_I60 'O curso disponibilizou monitores ou tutores para auxiliar os estudantes.'.
VARIABLE LABELS QE_I61 'As condições de infraestrutura das salas de aula foram adequadas.'.
VARIABLE LABELS QE_I62 'Os equipamentos e materiais disponíveis para as aulas práticas foram adequados para a quantidade de estudantes.'.
VARIABLE LABELS QE_I63 'Os ambientes e equipamentos destinados às aulas práticas foram adequados ao curso.'.
VARIABLE LABELS QE_I64 'A biblioteca dispôs das referências bibliográficas que os estudantes necessitaram.'.
VARIABLE LABELS QE_I65 'A instituição contou com biblioteca virtual ou conferiu acesso a obras disponíveis em acervos virtuais.'.
VARIABLE LABELS QE_I66 'As atividades acadêmicas desenvolvidas dentro e fora da sala de aula possibilitaram reflexão, convivência e respeito à diversidade.'.
VARIABLE LABELS QE_I67 'A instituição promoveu atividades de cultura, de lazer e interação social.'.
VARIABLE LABELS QE_I68 'A instituição dispôs de refeitório, cantina e banheiros em condições adequadas que atenderam as necessidades dos seus usuários.'.

