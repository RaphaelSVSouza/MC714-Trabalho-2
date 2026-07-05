# References

## 1. Fontes academicas dos algoritmos

### Relogio logico de Lamport

- Referencia principal: LAMPORT, Leslie. *Time, Clocks, and the Ordering of Events in a Distributed System*. Communications of the ACM, v. 21, n. 7, p. 558-565, 1978. DOI: 10.1145/359545.359563.
- Conceito utilizado: contador logico inteiro para eventos locais, envios e recebimentos; se `a -> b`, entao o timestamp logico de `a` e menor que o de `b`.
- Arquivos do projeto: `src/node/clock.py`, `src/node/state.py`, `src/node/models.py`, `src/node/main.py`, `scripts/smoke_lamport.py`, `scripts/run_experiments.py`.
- Adaptacao para HTTP: cada mensagem passa por `POST /messages`; o timestamp logico viaja no envelope JSON.
- Simplificacoes adotadas: um contador por no; lock local unico para estado relacionado; confirmacao tecnica de HTTP nao cria evento logico.
- Diferencas entre teoria e implementacao: a ordenacao e demonstrada em HTTP/JSON entre conteineres locais, nao em uma pilha de mensagens abstrata.
- Limitacoes conhecidas: `C(a) < C(b)` nao prova causalidade por si so; a inversa da implicacao permanece falsa.
- Seccao/pagina: `A confirmar na leitura integral do artigo`.

### Ricart-Agrawala

- Referencia principal: RICART, Glenn; AGRAWALA, Ashok K. *An Optimal Algorithm for Mutual Exclusion in Computer Networks*. Communications of the ACM, v. 24, n. 1, p. 9-17, 1981. DOI: 10.1145/358527.358537.
- Conceito utilizado: exclusao mutua distribuida por `REQUEST`/`REPLY`; prioridade pelo par `(request_timestamp, node_id)`; resposta adiada quando o receptor esta em `HELD` ou quando a prioridade local em `WANTED` e maior.
- Arquivos do projeto: `src/node/mutex.py`, `src/node/state.py`, `src/node/main.py`, `src/resource/main.py`, `scripts/smoke_mutex.py`, `scripts/run_experiments.py`, `tests/unit/test_mutex.py`, `tests/unit/test_state.py`.
- Adaptacao para HTTP: os pedidos e respostas sao mensagens HTTP/JSON enviadas para `/messages`; `request_timestamp` permanece fixo durante uma tentativa, enquanto `logical_time` varia por envio.
- Simplificacoes adotadas: conjunto fixo de tres nos; um pedido local por vez; participantes ativos durante a rodada; timeout apenas para limpeza local e teste.
- Diferencas entre teoria e implementacao: o projeto usa um observador externo `resource` apenas para registrar entrada, saida e sobreposicao; ele nao concede permissao.
- Limitacoes conhecidas: a falha de um participante pode impedir progresso ate timeout; isso nao e tratado como tolerancia a falhas.
- Seccao/pagina: `A confirmar na leitura integral do artigo`.

### Bully

- Referencia principal: GARCIA-MOLINA, Hector. *Elections in a Distributed Computing System*. IEEE Transactions on Computers, v. C-31, n. 1, p. 48-59, 1982. DOI: 10.1109/TC.1982.1675885.
- Conceito utilizado: processos com identificadores totalmente ordenados; o maior ID ativo torna-se coordenador.
- Arquivos do projeto: `src/node/election.py`, `src/node/state.py`, `src/node/main.py`, `scripts/smoke_election.py`, `scripts/run_experiments.py`, `tests/unit/test_election.py`.
- Adaptacao para HTTP: `ELECTION`, `ELECTION_OK`, `COORDINATOR` e `HEARTBEAT` sao mensagens HTTP/JSON do envelope comum do projeto.
- Simplificacoes adotadas: tres nos fixos; falha por parada real de conteiner; uma task de eleicao controlada por no; convergencia observada por polling.
- Diferencas entre teoria e implementacao: heartbeat foi adotado como detector local de falha para a demo com Docker; os nomes das mensagens sao uma adaptacao didatica ao envelope HTTP.
- Limitacoes conhecidas: nao ha tolerancia a particoes; atrasos arbitrariamente grandes podem quebrar as suposicoes de timeout; o lider nao coordena o mutex.
- Seccao/pagina: `A confirmar na leitura integral do artigo`.

## 2. Documentacao oficial das ferramentas

- Python: https://docs.python.org/3/library/asyncio-sync.html e https://docs.python.org/3/library/time.html#time.monotonic
  - Usado para `asyncio.Lock`, `asyncio.Event` e `time.monotonic()`.
- FastAPI: https://fastapi.tiangolo.com/
  - Usado para criar os endpoints HTTP dos nos e do observador.
- Uvicorn: https://www.uvicorn.org/
  - Usado para executar as aplicacoes ASGI.
- HTTPX: https://www.python-httpx.org/
  - Usado para chamadas HTTP entre processos e nos scripts de smoke e experimentos.
- Pytest: https://docs.pytest.org/en/stable/
  - Usado para a suite unitaria.
- Docker: https://docs.docker.com/
  - Usado para construir a imagem e executar os conteineres locais.
- Docker Compose: https://docs.docker.com/compose/
  - Usado para orquestrar `node1`, `node2`, `node3` e `resource`.

## 3. Codigo externo utilizado e alteracoes

### Declaracao de auditoria

Nenhum codigo externo foi incorporado. A auditoria do repositório nao encontrou trechos copiados de GitHub, Stack Overflow, blogs ou outros exercicios.

### Registro de alteracoes em codigo externo

Nao se aplica, porque nenhum codigo externo foi incluido.

### Formato para uso futuro

Se algum codigo externo for usado depois, registrar:

- titulo da fonte;
- autor ou organizacao;
- endereco ou DOI;
- data de acesso;
- arquivo afetado;
- trecho ou ideia utilizada;
- motivo do uso;
- alteracoes realizadas;
- licenca, quando existir.

## 4. Declaracao de implementacao propria

Nenhuma implementacao externa dos algoritmos foi incorporada. O codigo foi desenvolvido especificamente para este trabalho com base nas referencias academicas indicadas e na documentacao oficial listada acima.
