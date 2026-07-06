# Decision Log

## 2026-07-04 - HTTP/JSON como comunicação entre nós

- Decisao: usar HTTP/JSON para todas as mensagens entre processos.
- Alternativas consideradas: MPI/mpi4py e comunicação por arquivos.
- Justificativa: mantem a arquitetura aprovada e permite observar mensagens reais entre contêineres.
- Impacto: cada no expoe endpoints HTTP e usa JSON como formato de mensagem.
- Fonte relevante: fonte local não auditavel neste repositório.

## 2026-07-04 - Docker Compose com um conteiner por no

- Decisao: executar `node1`, `node2` e `node3` como servicos separados no Docker Compose.
- Alternativas consideradas: varios objetos no mesmo processo ou execucao manual em terminais separados.
- Justificativa: garante processos independentes e execucao local reproduzivel.
- Impacto: peers usam nomes internos do Compose, como `http://node2:8000`.
- Fonte relevante: fonte local não auditavel neste repositório.

## 2026-07-04 - FastAPI, Uvicorn e HTTPX

- Decisao: usar FastAPI para endpoints, Uvicorn para servir a aplicacao e HTTPX para chamadas entre nós.
- Alternativas consideradas: bibliotecas HTTP de baixo nivel.
- Justificativa: sao as ferramentas fixadas para o trabalho.
- Impacto: o mesmo código ASGI roda em todos os nós.
- Fonte relevante: documentacao oficial das ferramentas.

## 2026-07-04 - Sem arquivos compartilhados para coordenação

- Decisao: manter estado apenas em memória local de cada processo.
- Alternativas consideradas: volumes, arquivos, banco de dados e filas.
- Justificativa: o trabalho exige comunicação distribuída real por mensagens.
- Impacto: `/state` e `/events` mostram apenas o estado local do no consultado.
- Fonte relevante: fonte local não auditavel neste repositório.

## 2026-07-04 - Visualizacao pelo terminal

- Decisao: registrar envio e recebimento no stdout.
- Alternativas consideradas: dashboard web e ferramentas de observabilidade.
- Justificativa: suficiente para o projeto e para a gravacao.
- Impacto: `docker compose logs` mostra a comunicação entre os nós.
- Fonte relevante: fonte local não auditavel neste repositório.

## 2026-07-04 - `request_timestamp` separado do timestamp de envio

- Decisao: manter `request_timestamp` fixo por tentativa de Ricart-Agrawala, separado de `logical_time` de cada mensagem HTTP.
- Alternativas consideradas: usar o timestamp individual de cada envio.
- Justificativa: a prioridade do mutex depende da tentativa, não da ordem de envio para cada peer.
- Impacto: todas as mensagens `MUTEX_REQUEST` de uma tentativa carregam o mesmo `request_timestamp`.
- Fonte relevante: Ricart e Agrawala (1981), `pp. 9-17`.

## 2026-07-04 - Observador externo não coordenador

- Decisao: adicionar `resource` somente para registrar entrada, saída e sobreposicoes.
- Alternativas consideradas: usar o observador como autorizador central.
- Justificativa: a permissao deve vir apenas de Ricart-Agrawala.
- Impacto: o observador não envia mensagens aos nós e não decide prioridade.
- Fonte relevante: fonte local não auditavel neste repositório.

## 2026-07-04 - Timeout apenas para limpeza local

- Decisao: usar timeout finito no endpoint de seção critica para evitar espera indefinida em testes.
- Alternativas consideradas: esperar indefinidamente por todas as respostas.
- Justificativa: torna testes e demonstracoes controlaveis.
- Impacto: timeout limpa o estado local, mas não e apresentado como tolerância a falhas.
- Fonte relevante: Ricart e Agrawala (1981), `pp. 9-17`.

## 2026-07-04 - Participantes fixos e ativos durante uma rodada

- Decisao: o projeto assume peers fixos e ativos para Ricart-Agrawala.
- Alternativas consideradas: membership dinamico ou deteccao de falhas.
- Justificativa: manter escopo academico pequeno e não antecipar Bully.
- Impacto: falha de participante pode impedir progresso ate timeout.
- Fonte relevante: Ricart e Agrawala (1981), `pp. 9-17`.

## 2026-07-04 - Falha do lider por parada real de conteiner

- Decisao: demonstrar falha do lider com `docker compose stop node3` e recuperacao com `docker compose start node3`.
- Alternativas consideradas: endpoint `/commands/fail` ou flag interna de no morto.
- Justificativa: a demonstração precisa parar realmente o processo e manter comunicação por HTTP real.
- Impacto: `scripts/smoke_election.py` controla apenas Docker Compose e endpoints publicos.
- Fonte relevante: Garcia-Molina (1982), `pp. 48-59`.

## 2026-07-04 - Heartbeat como detector local de falha

- Decisao: usar `HEARTBEAT` periodico do lider e timeout medido com `time.monotonic()` nos seguidores.
- Alternativas consideradas: verificar `/health` diretamente ou detectar falha por qualquer erro HTTP.
- Justificativa: heartbeat torna a falha visivel como mensagem distribuída e evita usar horario de parede para timeout.
- Impacto: o lider envia mensagens periodicas e seguidores iniciam eleição quando o limite expira.
- Fonte relevante: adaptacao do projeto com base no modelo de timeout do Bully.

## 2026-07-04 - Mensagens ELECTION, ELECTION_OK e COORDINATOR como adaptacao HTTP

- Decisao: representar o Bully com tipos JSON `ELECTION`, `ELECTION_OK` e `COORDINATOR` no envelope existente.
- Alternativas consideradas: criar endpoints especificos por mensagem.
- Justificativa: reusa a infraestrutura de transporte, logs e Lamport sem criar protocolo paralelo.
- Impacto: todos os tipos passam por `POST /messages` e carregam `logical_time`.
- Fonte relevante: Garcia-Molina (1982), `pp. 48-59`.

## 2026-07-04 - `election_id` para ignorar respostas antigas

- Decisao: cada rodada local gera um `election_id`, e `ELECTION_OK` so e aceito se corresponder a rodada atual.
- Alternativas consideradas: aceitar qualquer resposta de no maior.
- Justificativa: mensagens atrasadas não devem concluir uma eleição nova.
- Impacto: `ELECTION` e `ELECTION_OK` carregam `election_id`; respostas duplicadas ou antigas geram evento ignorado.
- Fonte relevante: adaptacao do projeto para HTTP/JSON assincrono.

## 2026-07-04 - Eleição no startup para recuperar maior ID

- Decisao: cada no inicia uma eleição apos `STARTUP_ELECTION_DELAY_MS`.
- Alternativas consideradas: esperar apenas timeout de heartbeat.
- Justificativa: quando `node3` retorna, ele deve voltar a assumir por possuir o maior ID.
- Impacto: startup simultaneo pode gerar eleicoes concorrentes, mas converge para o maior ID ativo.
- Fonte relevante: Garcia-Molina (1982), `pp. 48-59`.

## 2026-07-04 - Coleta de experimentos em JSON e CSV

- Decisao: guardar os resultados dos experimentos em `docs/results/raw-results.json` e `docs/results/experiment-summary.csv`.
- Alternativas consideradas: manter apenas saída no terminal ou gerar planilhas externas.
- Justificativa: o relatorio precisa de dados reproduziveis e auditaveis.
- Impacto: `scripts/run_experiments.py` gera e reescreve os artefatos a partir da pilha real.
- Fonte relevante: requisito local não auditavel neste repositório.

## 2026-07-04 - Script unico de demonstração

- Decisao: manter `scripts/demo.py` como encadeamento simples dos smoke tests.
- Alternativas consideradas: criar um framework novo de demo.
- Justificativa: reduz complexidade e reaproveita os cenarios ja validados.
- Impacto: uma unica chamada mostra os principais marcos sem editar código.
- Fonte relevante: requisito local não auditavel neste repositório.

## 2026-07-04 - Captura UTF-8 em subprocessos do experimento

- Decisao: ler a saída do `docker compose` como UTF-8 com substituicao de erros no script de experimentos.
- Alternativas consideradas: captura padrao do Windows.
- Justificativa: evita falha de decode ao coletar logs do Compose no ambiente local.
- Impacto: `scripts/run_experiments.py` roda de forma mais estavel no Windows.
- Fonte relevante: bug concreto observado durante a consolidação final.

## 2026-07-05 - Auditoria e correcoes de prioridade, eventos e rastreabilidade

- Decisao: corrigir a aceitação de coordenador e heartbeat para respeitar o ID do proprio no, o lider atual e a prioridade anunciada.
- Alternativas consideradas: manter a excecao baseada em eleição ativa.
- Justificativa: uma eleição em andamento não pode anular a regra de prioridade do Bully.
- Impacto: mensagens inferiores sao rejeitadas, e mensagens inferiores coerentes podem disparar nova eleição quando apropriado.
- Decisao adicional: classificar registros de `/events` como `lamport_event` ou `annotation`.
- Impacto adicional: o log de observabilidade não e tratado como correspondencia um-para-um com eventos Lamport.
- Decisao adicional: renomear abort e retry locais com nomes semanticos mais claros.
- Impacto adicional: `MUTEX_ABORTED` e `ELECTION_ROUND_TIMED_OUT` descrevem melhor as transicoes locais.
- Decisao adicional: declarar `pydantic` diretamente no `pyproject.toml`.
- Impacto adicional: a dependencia não fica ocultada por dependencias transitivas de FastAPI.
- Decisao adicional: versionar os artefatos de resultados em `docs/results/`.
- Impacto adicional: o README e os arquivos CSV/JSON podem ser auditados em conjunto.
- Fonte relevante: este repositório, as referencias acadêmicas indicadas no prompt e a documentacao oficial das ferramentas.
