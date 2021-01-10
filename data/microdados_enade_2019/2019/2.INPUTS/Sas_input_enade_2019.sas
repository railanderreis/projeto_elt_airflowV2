/***************************************************************************** 
MEC/INEP/DAES (Diretoria de Avaliação da Educação Superior) 
Coordenação Geral de Controle de Qualidade da Educação Superior
------------------------------------------------------------------------------ 
Programa  : 
	sas_input_enade_2019.sas (Pasta "INPUTS") 
------------------------------------------------------------------------------ 
Descrição  : 
	Programa para Leitura dos Microdados do enade 2019

****************************************************************************** 
Obs  : Para executar este programa é necessário salvar o arquivo 
"microdados_enade_2019.txt" (Pasta "DADOS") no diretório "C  :\" do computador.
 
******************************************************************************/
DATA WORK.microdados_enade_2019;
	    INFILE "C:/microdados_enade_2019.txt"
		LRECL=539
		ENCODING="LATIN1"
		TERMSTR=CRLF
		DLM='7F'x
		MISSOVER
		DSD ;
   INPUT
        NU_ANO               : ?? BEST4.     /* Ano de realização do exame */
        CO_IES               : ?? BEST5.     /* Código da IES (e-MEC) */
        CO_CATEGAD           : ?? BEST5.     /* Código da categoria administrativa da IES */
        CO_ORGACAD           : ?? BEST5.     /* Código da organização acadêmica da IES */
        CO_GRUPO             : ?? BEST4.     /* Código da Área de enquadramento do curso no Enade */
        CO_CURSO             : ?? BEST7.     /* Código do curso no Enade */
        CO_MODALIDADE        : ?? BEST1.     /* Código da Modalidade de Ensino */
        CO_MUNIC_CURSO       : ?? BEST7.     /* Código do município de funcionamento do curso */
        CO_UF_CURSO          : ?? BEST2.     /* Código da UF de funcionamento do curso */
        CO_REGIAO_CURSO      : ?? BEST1.     /* Código da região de funcionamento do curso */
        NU_IDADE             : ?? BEST3.     /* Idade do inscrito em 24/11/2019 */
        TP_SEXO              : $CHAR1.       /* Sexo */
        ANO_FIM_2G           : ?? BEST4.     /* Ano de conclusão do Ensino Médio */
        ANO_IN_GRAD          : ?? BEST4.     /* Ano de início da graduação */
        CO_TURNO_GRADUACAO   : ?? BEST1.     /* Código do turno de graduação */
        TP_INSCRICAO_ADM     : ?? BEST1.     /* Forma pela qual foi realizada a inscrição */
        TP_INSCRICAO         : ?? BEST1.     /* Tipo de inscrição */
        NU_ITEM_OFG          : ?? BEST1.     /* Número de itens da parte objetiva da Formação Geral */
        NU_ITEM_OFG_Z        : ?? BEST1.     /* Número de itens da parte objetiva da Formação Geral que foram excluídos devido a anulação */
        NU_ITEM_OFG_X        : ?? BEST1.     /* Número de itens da parte objetiva da Formação Geral que foram excluídos devido ao coeficiente ponto-bisserial menor que 0,20 */
        NU_ITEM_OFG_N        : ?? BEST1.     /* Número de itens da parte objetiva da Formação Geral que não se aplicam ao grupo de curso */
        NU_ITEM_OCE          : ?? BEST2.     /* Número de itens da parte objetiva de Componente Específico */
        NU_ITEM_OCE_Z        : ?? BEST2.     /* Número de itens da parte objetiva de Componente Específico que foram excluídos devido a anulação */
        NU_ITEM_OCE_X        : ?? BEST2.     /* Número de itens da parte objetiva de Componente Específico que foram excluídos devido ao coeficiente ponto-bisserial menor que 0,20 */
        NU_ITEM_OCE_N        : ?? BEST2.     /* Número de itens da parte objetiva de Componente Específico que não se aplicam ao grupo de curso */
        DS_VT_GAB_OFG_ORIG   : $CHAR8.       /* Vetor que representa o gabarito original de Formação Geral */
        DS_VT_GAB_OFG_FIN    : $CHAR8.       /* Vetor que representa o gabarito final de Formação Geral */
        DS_VT_GAB_OCE_ORIG   : $CHAR27.      /* Vetor que representa o gabarito original de Componente Específico */
        DS_VT_GAB_OCE_FIN    : $CHAR27.      /* Vetor que representa o gabarito final de Componente Específico */
        DS_VT_ESC_OFG        : $CHAR8.       /* Vetor que representa a escolha de resposta da parte objetiva da Formação Geral */
        DS_VT_ACE_OFG        : $CHAR8.       /* Vetor que representa os acertos da parte objetiva na Formação Geral */
        DS_VT_ESC_OCE        : $CHAR27.      /* Vetor que representa a escolha de resposta da parte objetiva do Componente Específico */
        DS_VT_ACE_OCE        : $CHAR27.      /* Vetor que representa os acertos da parte objetiva do Componente Específico */
        TP_PRES              : ?? BEST3.     /* Tipo de presença no Enade */
        TP_PR_GER            : ?? BEST3.     /* Tipo de presença na prova */
        TP_PR_OB_FG          : ?? BEST3.     /* Tipo de presença na parte objetiva na formação geral */
        TP_PR_DI_FG          : ?? BEST3.     /* Tipo de presença na parte discursiva na formação geral */
        TP_PR_OB_CE          : ?? BEST3.     /* Tipo de presença na parte objetiva no componente específico */
        TP_PR_DI_CE          : ?? BEST3.     /* Tipo de presença na parte discursiva no componente específico */
        TP_SFG_D1            : ?? BEST3.     /* Tipo de situação da questão 1 da parte discursiva da formação geral */
        TP_SFG_D2            : ?? BEST3.     /* Tipo de situação da questão 2 da parte discursiva da formação geral */
        TP_SCE_D1            : ?? BEST3.     /* Tipo de situação da questão 1 da parte discursiva do componente específico */
        TP_SCE_D2            : ?? BEST3.     /* Tipo de situação da questão 2 da parte discursiva do componente específico */
        TP_SCE_D3            : ?? BEST3.     /* Tipo de situação da questão 3 da parte discursiva do componente específico */
        NT_GER               : ?? COMMA4.    /* Nota bruta da prova - Média ponderada da formação geral (25%) e componente específico (75%). (valor de 0 a 100) */
        NT_FG                : ?? COMMA4.    /* Nota bruta na formação geral - Média ponderada da parte objetiva (60%) e discursiva (40%) na formação geral. (valor de 0 a 100) */
        NT_OBJ_FG            : ?? COMMA4.    /* Nota bruta na parte objetiva da formação geral. (valor de 0 a 100) */
        NT_DIS_FG            : ?? COMMA4.    /* Nota bruta na parte discursiva da formação geral. (valor de 0 a 100) */
        NT_FG_D1             : ?? COMMA3.    /* Nota da questão 1 da parte discursiva da formação geral - Média ponderada da parte de Língua Portuguesa (20%) e Conteúdo (80%) da Questão 1 da parte discursiva. (valor de 0 a 100) */
        NT_FG_D1_PT          : ?? COMMA3.    /* Nota de Língua Portuguesa da questão 1 da parte discursiva da formação geral. (valor de 0 a 100) */
        NT_FG_D1_CT          : ?? COMMA3.    /* Nota de Conteúdo da questão 1 da parte discursiva da formação geral. (valor de 0 a 100) */
        NT_FG_D2             : ?? COMMA3.    /* Nota da questão 2 da parte discursiva na formação geral - Média ponderada da parte de Língua Portuguesa (20%) e Conteúdo (80%) da Questão 2 da parte discursiva. (valor de 0 a 100) */
        NT_FG_D2_PT          : ?? COMMA3.    /* Nota de Língua Portuguesa da questão 2 da parte discursiva da formação geral. (valor de 0 a 100) */
        NT_FG_D2_CT          : ?? COMMA3.    /* Nota de Conteúdo da questão 2 da parte discursiva da formação geral. (valor de 0 a 100) */
        NT_CE                : ?? COMMA4.    /* Nota bruta no componente específico - Média ponderada da parte objetiva (85%) e discursiva (15%) no componente específico. (valor de 0 a 100) */
        NT_OBJ_CE            : ?? COMMA4.    /* Nota bruta na parte objetiva do componente específico. (valor de 0 a 100) */
        NT_DIS_CE            : ?? COMMA4.    /* Nota bruta na parte discursiva do componente específico. (valor de 0 a 100) */
        NT_CE_D1             : ?? COMMA3.    /* Nota da questão 1 da parte discursiva do componente específico. (valor de 0 a 100) */
        NT_CE_D2             : ?? COMMA3.    /* Nota da questão 2 da parte discursiva do componente específico. (valor de 0 a 100) */
        NT_CE_D3             : ?? COMMA3.    /* Nota da questão 3 da parte discursiva do componente específico. (valor de 0 a 100) */
        CO_RS_I1             : $CHAR1.       /* 1 - Qual o grau de dificuldade desta prova na parte de Formação Geral? */
        CO_RS_I2             : $CHAR1.       /* 2 - Qual o grau de dificuldade desta prova na parte do Componente Específico? */
        CO_RS_I3             : $CHAR1.       /* 3 - Considerando a extensão da prova, em relação ao tempo total, você considera que a prova foi: */
        CO_RS_I4             : $CHAR1.       /* 4 - Os enunciados das questões da prova na parte de Formação Geral estavam claros e objetivos? */
        CO_RS_I5             : $CHAR1.       /* 5 - Os enunciados das questões na parte do Componente Específico estavam claros e objetivos? */
        CO_RS_I6             : $CHAR1.       /* 6 - As informações/instruções fornecidas para a resolução das questões foram suficientes para resolvê-las? */
        CO_RS_I7             : $CHAR1.       /* 7 - Você se deparou com alguma dificuldade ao responder à prova. Qual? */
        CO_RS_I8             : $CHAR1.       /* 8 - Considerando apenas as questões objetivas da prova, você percebeu que: */
        CO_RS_I9             : $CHAR1.       /* 9 - Qual foi o tempo gasto por você para concluir a prova? */
        QE_I01               : $CHAR1.       /* Qual o seu estado civil? */
        QE_I02               : $CHAR1.       /* Qual é a sua cor ou raça? */
        QE_I03               : $CHAR1.       /* Qual a sua nacionalidade? */
        QE_I04               : $CHAR1.       /* Até que etapa de escolarização seu pai concluiu? */
        QE_I05               : $CHAR1.       /* Até que etapa de escolarização sua mãe concluiu? */
        QE_I06               : $CHAR1.       /* Onde e com quem você mora atualmente? */
        QE_I07               : $CHAR1.       /* Quantas pessoas da sua família moram com você? Considere seus pais, irmãos, cônjuge, filhos e outros parentes que moram na mesma casa com você. */
        QE_I08               : $CHAR1.       /* Qual a renda total de sua família, incluindo seus rendimentos? */
        QE_I09               : $CHAR1.       /* Qual alternativa a seguir melhor descreve sua situação financeira (incluindo bolsas)? */
        QE_I10               : $CHAR1.       /* Qual alternativa a seguir melhor descreve sua situação de trabalho (exceto estágio ou bolsas)? */
        QE_I11               : $CHAR1.       /* Que tipo de bolsa de estudos ou financiamento do curso você recebeu para custear todas ou a maior parte das mensalidades? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração. */
        QE_I12               : $CHAR1.       /* Ao longo da sua trajetória acadêmica, você recebeu algum tipo de bolsa de permanência? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração. */
        QE_I13               : $CHAR1.       /* Ao longo da sua trajetória acadêmica, você recebeu algum tipo de bolsa acadêmica? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração. */
        QE_I14               : $CHAR1.       /* Durante o curso de graduação, você participou de programas e ou atividades curriculares no exterior? */
        QE_I15               : $CHAR1.       /* Seu ingresso no curso de graduação se deu por meio de políticas de ação afirmativa ou inclusão social? */
        QE_I16               : ?? BEST2.     /* Em que unidade da Federação você concluiu o ensino médio? */
        QE_I17               : $CHAR1.       /* Em que tipo de escola você cursou o ensino médio? */
        QE_I18               : $CHAR1.       /* Qual modalidade de ensino médio você concluiu? */
        QE_I19               : $CHAR1.       /* Quem mais lhe incentivou a cursar a graduação? */
        QE_I20               : $CHAR1.       /* Algum dos grupos abaixo foi determinante para você enfrentar dificuldades durante seu curso superior e conclui-lo? */
        QE_I21               : $CHAR1.       /* Alguém em sua família concluiu um curso superior? */
        QE_I22               : $CHAR1.       /* Excetuando-se os livros indicados na bibliografia do seu curso, quantos livros você leu neste ano? */
        QE_I23               : $CHAR1.       /* Quantas horas por semana, aproximadamente, você dedicou aos estudos, excetuando as horas de aula? */
        QE_I24               : $CHAR1.       /* Você teve oportunidade de aprendizado de idioma estrangeiro na Instituição? */
        QE_I25               : $CHAR1.       /* Qual o principal motivo para você ter escolhido este curso? */
        QE_I26               : $CHAR1.       /* Qual a principal razão para você ter escolhido a sua instituição de educação superior? */
        QE_I27               : ?? BEST1.     /* As disciplinas cursadas contribuíram para sua formação integral, como cidadão e profissional. */
        QE_I28               : ?? BEST1.     /* Os conteúdos abordados nas disciplinas do curso favoreceram sua atuação em estágios ou em atividades de iniciação profissional. */
        QE_I29               : ?? BEST1.     /* As metodologias de ensino utilizadas no curso desafiaram você a aprofundar conhecimentos e desenvolver competências reflexivas e críticas. */
        QE_I30               : ?? BEST1.     /* O curso propiciou experiências de aprendizagem inovadoras. */
        QE_I31               : ?? BEST1.     /* O curso contribuiu para o desenvolvimento da sua consciência ética para o exercício profissional. */
        QE_I32               : ?? BEST1.     /* No curso você teve oportunidade de aprender a trabalhar em equipe. */
        QE_I33               : ?? BEST1.     /* O curso possibilitou aumentar sua capacidade de reflexão e argumentação. */
        QE_I34               : ?? BEST1.     /* O curso promoveu o desenvolvimento da sua capacidade de pensar criticamente, analisar e refletir sobre soluções para problemas da sociedade. */
        QE_I35               : ?? BEST1.     /* O curso contribuiu para você ampliar sua capacidade de comunicação nas formas oral e escrita. */
        QE_I36               : ?? BEST1.     /* O curso contribuiu para o desenvolvimento da sua capacidade de aprender e atualizar-se permanentemente. */
        QE_I37               : ?? BEST1.     /* As relações professor-aluno ao longo do curso estimularam você a estudar e aprender. */
        QE_I38               : ?? BEST1.     /* Os planos de ensino apresentados pelos professores contribuíram para o desenvolvimento das atividades acadêmicas e para seus estudos. */
        QE_I39               : ?? BEST1.     /* As referências bibliográficas indicadas pelos professores nos planos de ensino contribuíram para seus estudos e aprendizagens. */
        QE_I40               : ?? BEST1.     /* Foram oferecidas oportunidades para os estudantes superarem problemas e dificuldades relacionados ao processo de formação. */
        QE_I41               : ?? BEST1.     /* A coordenação do curso esteve disponível para orientação acadêmica dos estudantes. */
        QE_I42               : ?? BEST1.     /* O curso exigiu de você organização e dedicação frequente aos estudos. */
        QE_I43               : ?? BEST1.     /* Foram oferecidas oportunidades para os estudantes participarem de programas, projetos ou atividades de extensão universitária. */
        QE_I44               : ?? BEST1.     /* Foram oferecidas oportunidades para os estudantes participarem de projetos de iniciação científica e de atividades que estimularam a investigação acadêmica. */
        QE_I45               : ?? BEST1.     /* O curso ofereceu condições para os estudantes participarem de eventos internos e/ou externos à instituição. */
        QE_I46               : ?? BEST1.     /* A instituição ofereceu oportunidades para os estudantes atuarem como representantes em órgãos colegiados. */
        QE_I47               : ?? BEST1.     /* O curso favoreceu a articulação do conhecimento teórico com atividades práticas. */
        QE_I48               : ?? BEST1.     /* As atividades práticas foram suficientes para relacionar os conteúdos do curso com a prática, contribuindo para sua formação profissional. */
        QE_I49               : ?? BEST1.     /* O curso propiciou acesso a conhecimentos atualizados e/ou contemporâneos em sua área de formação. */
        QE_I50               : ?? BEST1.     /* O estágio supervisionado proporcionou experiências diversificadas para a sua formação. */
        QE_I51               : ?? BEST1.     /* As atividades realizadas durante seu trabalho de conclusão de curso contribuíram para qualificar sua formação profissional. */
        QE_I52               : ?? BEST1.     /* Foram oferecidas oportunidades para os estudantes realizarem intercâmbios e/ou estágios no país.  */
        QE_I53               : ?? BEST1.     /* Foram oferecidas oportunidades para os estudantes realizarem intercâmbios e/ou estágios fora do país. */
        QE_I54               : ?? BEST1.     /* Os estudantes participaram de avaliações periódicas do curso (disciplinas, atuação dos professores, infraestrutura). */
        QE_I55               : ?? BEST1.     /* As avaliações de aprendizagem realizadas durante o curso foram compatíveis com os conteúdos ou temas trabalhados pelos professores. */
        QE_I56               : ?? BEST1.     /* Os professores apresentaram disponibilidade para atender os estudantes fora do horário das aulas. */
        QE_I57               : ?? BEST1.     /* Os professores demonstraram domínio dos conteúdos abordados nas disciplinas. */
        QE_I58               : ?? BEST1.     /* Os professores utilizaram tecnologias da informação e comunicação (TIC) como estratégia de ensino (projetor, multimídia, laboratório de informática, ambiente virtual de aprendizagem). */
        QE_I59               : ?? BEST1.     /* A instituição dispôs de quantidade suficiente de funcionários para o apoio administrativo e acadêmico. */
        QE_I60               : ?? BEST1.     /* O curso disponibilizou monitores ou tutores para auxiliar os estudantes. */
        QE_I61               : ?? BEST1.     /* As condições de infraestrutura das salas de aula foram adequadas. */
        QE_I62               : ?? BEST1.     /* Os equipamentos e materiais disponíveis para as aulas práticas foram adequados para a quantidade de estudantes. */
        QE_I63               : ?? BEST1.     /* Os ambientes e equipamentos destinados às aulas práticas foram adequados ao curso. */
        QE_I64               : ?? BEST1.     /* A biblioteca dispôs das referências bibliográficas que os estudantes necessitaram. */
        QE_I65               : ?? BEST1.     /* A instituição contou com biblioteca virtual ou conferiu acesso a obras disponíveis em acervos virtuais. */
        QE_I66               : ?? BEST1.     /* As atividades acadêmicas desenvolvidas dentro e fora da sala de aula possibilitaram reflexão, convivência e respeito à diversidade. */
        QE_I67               : ?? BEST1.     /* A instituição promoveu atividades de cultura, de lazer, de convívio e interação social. */
        QE_I68               : ?? BEST1.     /* A instituição dispôs de refeitório, cantina e banheiros em condições adequadas que atenderam as necessidades dos seus usuários. */
        ;
   LABEL
        NU_ANO =  Ano de realização do exame
        CO_IES =  Código da IES (e-MEC)
        CO_CATEGAD =  Código da categoria administrativa da IES
        CO_ORGACAD =  Código da organização acadêmica da IES
        CO_GRUPO =  Código da Área de enquadramento do curso no Enade
        CO_CURSO =  Código do curso no Enade
        CO_MODALIDADE =  Código da Modalidade de Ensino
        CO_MUNIC_CURSO =  Código do município de funcionamento do curso
        CO_UF_CURSO =  Código da UF de funcionamento do curso
        CO_REGIAO_CURSO =  Código da região de funcionamento do curso
        NU_IDADE =  Idade do inscrito em 24/11/2019
        TP_SEXO =  Sexo
        ANO_FIM_2G =  Ano de conclusão do Ensino Médio
        ANO_IN_GRAD =  Ano de início da graduação
        CO_TURNO_GRADUACAO =  Código do turno de graduação
        TP_INSCRICAO _ADM =  Forma pela qual foi realizada a inscrição
        TP_INSCRICAO =  Tipo de inscrição
        NU_ITEM_OFG =  Número de itens da parte objetiva da Formação Geral
        NU_ITEM_OFG_Z =  Número de itens da parte objetiva da Formação Geral que foram excluídos devido a anulação
        NU_ITEM_OFG_X =  Número de itens da parte objetiva da Formação Geral que foram excluídos devido ao coeficiente ponto-bisserial menor que 0,20
        NU_ITEM_OFG_N =  Número de itens da parte objetiva da Formação Geral que não se aplicam ao grupo de curso
        NU_ITEM_OCE =  Número de itens da parte objetiva de Componente Específico
        NU_ITEM_OCE_Z =  Número de itens da parte objetiva de Componente Específico que foram excluídos devido a anulação
        NU_ITEM_OCE_X =  Número de itens da parte objetiva de Componente Específico que foram excluídos devido ao coeficiente ponto-bisserial menor que 0,20
        NU_ITEM_OCE_N =  Número de itens da parte objetiva de Componente Específico que não se aplicam ao grupo de curso
        DS_VT_GAB_OFG_ORIG =  Vetor que representa o gabarito original de Formação Geral
        DS_VT_GAB_OFG_FIN =  Vetor que representa o gabarito final de Formação Geral
        DS_VT_GAB_OCE_ORIG =  Vetor que representa o gabarito original de Componente Específico
        DS_VT_GAB_OCE_FIN =  Vetor que representa o gabarito final de Componente Específico
        DS_VT_ESC_OFG =  Vetor que representa a escolha de resposta da parte objetiva da Formação Geral
        DS_VT_ACE_OFG =  Vetor que representa os acertos da parte objetiva na Formação Geral
        DS_VT_ESC_OCE =  Vetor que representa a escolha de resposta da parte objetiva do Componente Específico
        DS_VT_ACE_OCE =  Vetor que representa os acertos da parte objetiva do Componente Específico
        TP_PRES =  Tipo de presença no Enade
        TP_PR_GER =  Tipo de presença na prova
        TP_PR_OB_FG =  Tipo de presença na parte objetiva na formação geral
        TP_PR_DI_FG =  Tipo de presença na parte discursiva na formação geral
        TP_PR_OB_CE =  Tipo de presença na parte objetiva no componente específico
        TP_PR_DI_CE =  Tipo de presença na parte discursiva no componente específico
        TP_SFG_D1 =  Tipo de situação da questão 1 da parte discursiva da formação geral
        TP_SFG_D2 =  Tipo de situação da questão 2 da parte discursiva da formação geral
        TP_SCE_D1 =  Tipo de situação da questão 1 da parte discursiva do componente específico
        TP_SCE_D2 =  Tipo de situação da questão 2 da parte discursiva do componente específico
        TP_SCE_D3 =  Tipo de situação da questão 3 da parte discursiva do componente específico
        NT_GER =  Nota bruta da prova - Média ponderada da formação geral (25%) e componente específico (75%). (valor de 0 a 100)
        NT_FG =  Nota bruta na formação geral - Média ponderada da parte objetiva (60%) e discursiva (40%) na formação geral. (valor de 0 a 100)
        NT_OBJ_FG =  Nota bruta na parte objetiva da formação geral. (valor de 0 a 100)
        NT_DIS_FG =  Nota bruta na parte discursiva da formação geral. (valor de 0 a 100)
        NT_FG_D1 =  Nota da questão 1 da parte discursiva da formação geral - Média ponderada da parte de Língua Portuguesa (20%) e Conteúdo (80%) da Questão 1 da parte discursiva. (valor de 0 a 100)
        NT_FG_D1_PT =  Nota de Língua Portuguesa da questão 1 da parte discursiva da formação geral. (valor de 0 a 100)
        NT_FG_D1_CT =  Nota de Conteúdo da questão 1 da parte discursiva da formação geral. (valor de 0 a 100)
        NT_FG_D2 =  Nota da questão 2 da parte discursiva na formação geral - Média ponderada da parte de Língua Portuguesa (20%) e Conteúdo (80%) da Questão 2 da parte discursiva. (valor de 0 a 100)
        NT_FG_D2_PT =  Nota de Língua Portuguesa da questão 2 da parte discursiva da formação geral. (valor de 0 a 100)
        NT_FG_D2_CT =  Nota de Conteúdo da questão 2 da parte discursiva da formação geral. (valor de 0 a 100)
        NT_CE =  Nota bruta no componente específico - Média ponderada da parte objetiva (85%) e discursiva (15%) no componente específico. (valor de 0 a 100)
        NT_OBJ_CE =  Nota bruta na parte objetiva do componente específico. (valor de 0 a 100)
        NT_DIS_CE =  Nota bruta na parte discursiva do componente específico. (valor de 0 a 100)
        NT_CE_D1 =  Nota da questão 1 da parte discursiva do componente específico. (valor de 0 a 100)
        NT_CE_D2 =  Nota da questão 2 da parte discursiva do componente específico. (valor de 0 a 100)
        NT_CE_D3 =  Nota da questão 3 da parte discursiva do componente específico. (valor de 0 a 100)
        CO_RS_I1 =  1 - Qual o grau de dificuldade desta prova na parte de Formação Geral?
        CO_RS_I2 =  2 - Qual o grau de dificuldade desta prova na parte do Componente Específico?
        CO_RS_I3 =  3 - Considerando a extensão da prova, em relação ao tempo total, você considera que a prova foi:
        CO_RS_I4 =  4 - Os enunciados das questões da prova na parte de Formação Geral estavam claros e objetivos?
        CO_RS_I5 =  5 - Os enunciados das questões na parte do Componente Específico estavam claros e objetivos?
        CO_RS_I6 =  6 - As informações/instruções fornecidas para a resolução das questões foram suficientes para resolvê-las?
        CO_RS_I7 =  7 - Você se deparou com alguma dificuldade ao responder à prova. Qual?
        CO_RS_I8 =  8 - Considerando apenas as questões objetivas da prova, você percebeu que:
        CO_RS_I9 =  9 - Qual foi o tempo gasto por você para concluir a prova?
        QE_I01 =  Qual o seu estado civil?
        QE_I02 =  Qual é a sua cor ou raça?
        QE_I03 =  Qual a sua nacionalidade?
        QE_I04 =  Até que etapa de escolarização seu pai concluiu?
        QE_I05 =  Até que etapa de escolarização sua mãe concluiu?
        QE_I06 =  Onde e com quem você mora atualmente?
        QE_I07 =  Quantas pessoas da sua família moram com você? Considere seus pais, irmãos, cônjuge, filhos e outros parentes que moram na mesma casa com você.
        QE_I08 =  Qual a renda total de sua família, incluindo seus rendimentos?
        QE_I09 =  Qual alternativa a seguir melhor descreve sua situação financeira (incluindo bolsas)?
        QE_I10 =  Qual alternativa a seguir melhor descreve sua situação de trabalho (exceto estágio ou bolsas)?
        QE_I11 =  Que tipo de bolsa de estudos ou financiamento do curso você recebeu para custear todas ou a maior parte das mensalidades? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração.
        QE_I12 =  Ao longo da sua trajetória acadêmica, você recebeu algum tipo de bolsa de permanência? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração.
        QE_I13 =  Ao longo da sua trajetória acadêmica, você recebeu algum tipo de bolsa acadêmica? No caso de haver mais de uma opção, marcar apenas a bolsa de maior duração.
        QE_I14 =  Durante o curso de graduação, você participou de programas e ou atividades curriculares no exterior?
        QE_I15 =  Seu ingresso no curso de graduação se deu por meio de políticas de ação afirmativa ou inclusão social?
        QE_I16 =  Em que unidade da Federação você concluiu o ensino médio?
        QE_I17 =  Em que tipo de escola você cursou o ensino médio?
        QE_I18 =  Qual modalidade de ensino médio você concluiu?
        QE_I19 =  Quem mais lhe incentivou a cursar a graduação?
        QE_I20 =  Algum dos grupos abaixo foi determinante para você enfrentar dificuldades durante seu curso superior e conclui-lo?
        QE_I21 =  Alguém em sua família concluiu um curso superior?
        QE_I22 =  Excetuando-se os livros indicados na bibliografia do seu curso, quantos livros você leu neste ano?
        QE_I23 =  Quantas horas por semana, aproximadamente, você dedicou aos estudos, excetuando as horas de aula?
        QE_I24 =  Você teve oportunidade de aprendizado de idioma estrangeiro na Instituição?
        QE_I25 =  Qual o principal motivo para você ter escolhido este curso?
        QE_I26 =  Qual a principal razão para você ter escolhido a sua instituição de educação superior?
        QE_I27 =  As disciplinas cursadas contribuíram para sua formação integral, como cidadão e profissional.
        QE_I28 =  Os conteúdos abordados nas disciplinas do curso favoreceram sua atuação em estágios ou em atividades de iniciação profissional.
        QE_I29 =  As metodologias de ensino utilizadas no curso desafiaram você a aprofundar conhecimentos e desenvolver competências reflexivas e críticas.
        QE_I30 =  O curso propiciou experiências de aprendizagem inovadoras.
        QE_I31 =  O curso contribuiu para o desenvolvimento da sua consciência ética para o exercício profissional.
        QE_I32 =  No curso você teve oportunidade de aprender a trabalhar em equipe.
        QE_I33 =  O curso possibilitou aumentar sua capacidade de reflexão e argumentação.
        QE_I34 =  O curso promoveu o desenvolvimento da sua capacidade de pensar criticamente, analisar e refletir sobre soluções para problemas da sociedade.
        QE_I35 =  O curso contribuiu para você ampliar sua capacidade de comunicação nas formas oral e escrita.
        QE_I36 =  O curso contribuiu para o desenvolvimento da sua capacidade de aprender e atualizar-se permanentemente.
        QE_I37 =  As relações professor-aluno ao longo do curso estimularam você a estudar e aprender.
        QE_I38 =  Os planos de ensino apresentados pelos professores contribuíram para o desenvolvimento das atividades acadêmicas e para seus estudos.
        QE_I39 =  As referências bibliográficas indicadas pelos professores nos planos de ensino contribuíram para seus estudos e aprendizagens.
        QE_I40 =  Foram oferecidas oportunidades para os estudantes superarem problemas e dificuldades relacionados ao processo de formação.
        QE_I41 =  A coordenação do curso esteve disponível para orientação acadêmica dos estudantes.
        QE_I42 =  O curso exigiu de você organização e dedicação frequente aos estudos.
        QE_I43 =  Foram oferecidas oportunidades para os estudantes participarem de programas, projetos ou atividades de extensão universitária.
        QE_I44 =  Foram oferecidas oportunidades para os estudantes participarem de projetos de iniciação científica e de atividades que estimularam a investigação acadêmica.
        QE_I45 =  O curso ofereceu condições para os estudantes participarem de eventos internos e/ou externos à instituição.
        QE_I46 =  A instituição ofereceu oportunidades para os estudantes atuarem como representantes em órgãos colegiados.
        QE_I47 =  O curso favoreceu a articulação do conhecimento teórico com atividades práticas.
        QE_I48 =  As atividades práticas foram suficientes para relacionar os conteúdos do curso com a prática, contribuindo para sua formação profissional.
        QE_I49 =  O curso propiciou acesso a conhecimentos atualizados e/ou contemporâneos em sua área de formação.
        QE_I50 =  O estágio supervisionado proporcionou experiências diversificadas para a sua formação.
        QE_I51 =  As atividades realizadas durante seu trabalho de conclusão de curso contribuíram para qualificar sua formação profissional.
        QE_I52 =  Foram oferecidas oportunidades para os estudantes realizarem intercâmbios e/ou estágios no país. 
        QE_I53 =  Foram oferecidas oportunidades para os estudantes realizarem intercâmbios e/ou estágios fora do país.
        QE_I54 =  Os estudantes participaram de avaliações periódicas do curso (disciplinas, atuação dos professores, infraestrutura).
        QE_I55 =  As avaliações de aprendizagem realizadas durante o curso foram compatíveis com os conteúdos ou temas trabalhados pelos professores.
        QE_I56 =  Os professores apresentaram disponibilidade para atender os estudantes fora do horário das aulas.
        QE_I57 =  Os professores demonstraram domínio dos conteúdos abordados nas disciplinas.
        QE_I58 =  Os professores utilizaram tecnologias da informação e comunicação (TIC) como estratégia de ensino (projetor, multimídia, laboratório de informática, ambiente virtual de aprendizagem).
        QE_I59 =  A instituição dispôs de quantidade suficiente de funcionários para o apoio administrativo e acadêmico.
        QE_I60 =  O curso disponibilizou monitores ou tutores para auxiliar os estudantes.
        QE_I61 =  As condições de infraestrutura das salas de aula foram adequadas.
        QE_I62 =  Os equipamentos e materiais disponíveis para as aulas práticas foram adequados para a quantidade de estudantes.
        QE_I63 =  Os ambientes e equipamentos destinados às aulas práticas foram adequados ao curso.
        QE_I64 =  A biblioteca dispôs das referências bibliográficas que os estudantes necessitaram.
        QE_I65 =  A instituição contou com biblioteca virtual ou conferiu acesso a obras disponíveis em acervos virtuais.
        QE_I66 =  As atividades acadêmicas desenvolvidas dentro e fora da sala de aula possibilitaram reflexão, convivência e respeito à diversidade.
        QE_I67 =  A instituição promoveu atividades de cultura, de lazer, de convívio e interação social.
        QE_I68 =  A instituição dispôs de refeitório, cantina e banheiros em condições adequadas que atenderam as necessidades dos seus usuários.
        ;
RUN;