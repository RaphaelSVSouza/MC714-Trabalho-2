# MC714 - Trabalho 2

ImplementaÃ§Ã£o academica local de sistemas distribuidos com comunicaÃ§Ã£o HTTP/JSON real entre contÃªineres.

## VisÃ£o geral

O projeto preserva a arquitetura com trÃªs nÃ³s (`node1`, `node2`, `node3`) e um observador externo (`resource`). O estado permanece local e em memÃ³ria, a comunicaÃ§Ã£o usa FastAPI, Uvicorn e HTTPX, e a orquestraÃ§Ã£o ocorre por Docker Compose.

NÃ£o hÃ¡ consenso, Paxos, Raft, replicaÃ§Ã£o, persistÃªncia, autenticaÃ§Ã£o, frontend, dashboard, Kubernetes, banco de dados, tolerÃ¢ncia a partiÃ§Ãµes ou coordenaÃ§Ã£o por arquivos/volumes.

## Arquitetura

- `node1`, `node2` e `node3` executam o mesmo cÃ³digo em contÃªineres separados.
- `resource` e um observador externo, nÃ£o um coordenador.
- A comunicaÃ§Ã£o entre nÃ³s usa HTTP/JSON via FastAPI, Uvicorn e HTTPX.
- Cada nÃ³ possui estado local apenas em memÃ³ria.
- Docker Compose orquestra a pilha local.

Mapa rÃ¡pido:

```text
node1 <-> node2 <-> node3
   \        |        /
        HTTP/JSON
            |
        resource
```

## Algoritmos

### Lamport

O projeto trata como eventos Lamport os registros que avanÃ§am ou atualizam o relogio logico:

1. evento local: `clock = clock + 1`
2. envio: `clock = clock + 1` e o valor vai em `logical_time`
3. recebimento: `clock = max(clock, received) + 1`

O endpoint `/events` e um log de observabilidade. Ele pode conter anotacoes do mesmo acontecimento, e nÃ£o deve ser lido como uma lista estritamente um-para-um de eventos teoricos.

A causalidade demonstrada no projeto vale para os cenarios executados. Um timestamp menor nÃ£o prova causalidade.

### Ricart-Agrawala

- Os estados `RELEASED`, `WANTED` e `HELD` sao a modelagem local usada pelo projeto.
- A prioridade segue o par `(request_timestamp, node_id)`.
- As mensagens centrais sao `MUTEX_REQUEST` e `MUTEX_REPLY`.
- O custo esperado sem falhas e `2 x (N - 1)` mensagens do protocolo, contando apenas `REQUEST` e `REPLY`.
- O timeout existe para limpeza local e teste, nÃ£o para oferecer tolerÃ¢ncia a falhas.
- O observador `resource` nÃ£o concede acesso; ele apenas registra entrada, saÃ­da e sobreposicao.

### Bully

- As mensagens centrais do protocolo sao `ELECTION`, `ELECTION_OK` e `COORDINATOR`.
- `HEARTBEAT` e `election_id` sao adaptacoes do projeto.
- O maior `node_id` ativo vira lider.
- O lider nÃ£o autoriza o mutex.
- A eleiÃ§Ã£o depende de atrasos limitados e timeouts coerentes com a rede local.
- NÃ£o ha tolerÃ¢ncia a partiÃ§Ãµes.
- Se os timeouts forem incompatÃ­veis com a rede, o comportamento pode ficar inadequado.

ConfiguraÃ§Ã£o usada pela pilha:

```text
HEARTBEAT_INTERVAL_MS=700
LEADER_TIMEOUT_MS=2500
ELECTION_RESPONSE_TIMEOUT_MS=700
COORDINATOR_TIMEOUT_MS=2500
STARTUP_ELECTION_DELAY_MS=500
```

## Requisitos

- Python 3.12
- Docker
- Docker Compose

## Ambiente local

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[test]"
```

## Subir a pilha

```powershell
docker compose up --build -d
```

Servicos expostos no host:

- `node1`: `http://localhost:8001`
- `node2`: `http://localhost:8002`
- `node3`: `http://localhost:8003`
- `resource`: `http://localhost:8010`

Entre contÃªineres, use nomes do Compose, por exemplo `http://node2:8000`.

## Endpoints principais

```powershell
curl http://localhost:8001/health
curl http://localhost:8001/state
curl http://localhost:8002/events
curl http://localhost:8010/state
```

Enviar mensagem simples:

```powershell
curl -X POST http://localhost:8001/commands/send-message `
  -H "Content-Type: application/json" `
  -d "{\"destination_id\":2,\"text\":\"mensagem de teste\"}"
```

Evento local Lamport:

```powershell
curl -X POST http://localhost:8001/commands/local-event `
  -H "Content-Type: application/json" `
  -d "{\"description\":\"processamento interno\"}"
```

Solicitar mutex:

```powershell
curl -X POST http://localhost:8001/commands/request-critical-section `
  -H "Content-Type: application/json" `
  -d "{\"duration_ms\":300}"
```

Iniciar eleiÃ§Ã£o manual:

```powershell
curl -X POST http://localhost:8001/commands/start-election `
  -H "Content-Type: application/json" `
  -d "{\"reason\":\"demo\"}"
```

## Smoke tests

```powershell
python scripts/smoke_http.py
python scripts/smoke_lamport.py
python scripts/smoke_mutex.py
python scripts/smoke_election.py
```

Uso rÃ¡pido para demonstraÃ§Ã£o:

```powershell
python scripts/demo.py
```

## Experimentos

```powershell
python scripts/run_experiments.py
```

Os dados brutos ficam em `docs/results/raw-results.json` e o resumo em `docs/results/experiment-summary.csv`. O README de resultados em `docs/results/README.md` descreve a execucao mais recente gerada pelo script.

## Resultados coletados

Resumo da ultima execucao versionada dos experimentos:

| Experimento | Medida | Resultado |
| --- | --- | --- |
| Lamport | violacoes | 0 em 5 repeticoes |
| Mutex individual | mensagens do protocolo | 4 em 5 repeticoes |
| Mutex individual | espera media | 71.692 ms |
| Mutex concorrente | sobreposicoes | 0 em 15 pedidos |
| Mutex concorrente | respostas adiadas medias | 1 |
| EleiÃ§Ã£o | deteccao de parada do lider | 4293.6 ms media |
| EleiÃ§Ã£o | recuperacao do lider | 662.6 ms media |
| EleiÃ§Ã£o | divergencia apos estabilizacao | 0 |

## Logs e visualizacao

```powershell
docker compose logs --no-color
```

Exemplo de saÃ­da:

```text
wall=... | L=30 | node=1 | LEADER_TIMEOUT | leader=3
wall=... | L=31 | node=1 | ELECTION_STARTED | election=...
wall=... | L=32 | node=1 | SEND | peer=2 | type=ELECTION
wall=... | L=35 | node=2 | SEND | peer=1 | type=ELECTION_OK
wall=... | L=40 | node=2 | BECAME_LEADER | leader=2
wall=... | L=41 | node=2 | SEND | peer=1 | type=COORDINATOR
```

## Encerrar

```powershell
docker compose down
```

## Estrutura

```text
src/node/                     No HTTP com Lamport, mutex e eleiÃ§Ã£o
src/resource/                 Observador da seÃ§Ã£o critica
scripts/                      Smoke tests, demo e experimentos
tests/unit/                   Testes unitarios
docs/results/                 Resultados gerados por `scripts/run_experiments.py`
docs/references.md            Rastreabilidade academica
docs/algorithm-traceability.md  Matriz de rastreabilidade dos algoritmos
docs/decision-log.md          Decisoes relevantes
Dockerfile                    Imagem comum
docker-compose.yml            Pilha local com 4 servicos
pyproject.toml                Dependencias e config de testes
```

## Limitacoes

- Ricart-Agrawala continua dependente de participantes ativos durante a rodada.
- O timeout do mutex limpa estado local, mas nÃ£o prova tolerÃ¢ncia a falhas.
- Bully assume falha por parada real de processo e atrasos locais limitados.
- Particoes de rede nÃ£o sao tratadas.
- Mensagens inferiores sao rejeitadas mesmo se houver eleiÃ§Ã£o em andamento.
- O lider eleito nÃ£o coordena o mutex.
- Eventos e anotacoes ficam apenas em memÃ³ria local.
- O observador `resource` nÃ£o participa da seguranÃ§a.

## Referencias

Veja `docs/references.md` para a lista de fontes academicas, documentacao oficial e declaracao de autoria.
