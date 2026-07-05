# MC714 - Trabalho 2

Implementacao academica local e minimalista de sistemas distribuidos com comunicacao HTTP/JSON real entre conteineres.

## Estado atual

Marco 5 concluido. O projeto ficou congelado em:

- relogio logico de Lamport;
- exclusao mutua distribuida com Ricart-Agrawala;
- eleicao de lider com Bully;
- observador `resource` apenas para registrar a secao critica;
- scripts de smoke, demonstracao e experimentos;
- documentacao final, rastreabilidade e resultados coletados.

Nao ha consenso, Paxos, Raft, replicacao, persistencia, autenticacao, frontend, dashboard, Kubernetes, banco de dados, tolerancia a particoes ou coordenacao por arquivos/volumes.

## Arquitetura

- `node1`, `node2` e `node3` executam o mesmo codigo em conteineres separados.
- `resource` e um observador externo, nao um coordenador.
- A comunicacao entre nos usa HTTP/JSON via FastAPI, Uvicorn e HTTPX.
- Cada no possui estado local apenas em memoria.
- Docker Compose orquestra a pilha local.

Mapa rapido:

```text
node1 <-> node2 <-> node3
   \        |        /
        HTTP/JSON
            |
        resource
```

## Algoritmos

### Lamport

Regra local:

1. evento local: `clock = clock + 1`
2. envio: `clock = clock + 1` e o valor vai em `logical_time`
3. recebimento: `clock = max(clock, received) + 1`

### Ricart-Agrawala

- estados: `RELEASED`, `WANTED`, `HELD`
- prioridade: `(request_timestamp, node_id)`
- mensagens: `MUTEX_REQUEST` e `MUTEX_REPLY`
- custo esperado sem falhas: `2 x (N - 1)` mensagens do protocolo
- `resource` nao concede acesso; apenas observa entrada, saida e sobreposicao

### Bully

- maior `node_id` ativo vira lider
- mensagens: `ELECTION`, `ELECTION_OK`, `COORDINATOR`, `HEARTBEAT`
- falha demonstrada por `docker compose stop node3`
- recuperacao demonstrada por `docker compose start node3`
- o lider nao autoriza o mutex

Configuracao usada pela pilha:

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

Entre conteineres, use nomes do Compose, por exemplo `http://node2:8000`.

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

Iniciar eleicao manual:

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

Uso rapido para demonstracao:

```powershell
python scripts/demo.py
```

## Experimentos

```powershell
python scripts/run_experiments.py
```

Os dados brutos ficam em `docs/results/raw-results.json` e o resumo em `docs/results/experiment-summary.csv`.

## Resultados coletados

Resumo real da ultima execucao dos experimentos:

| Experimento | Medida | Resultado |
| --- | --- | --- |
| Lamport | violacoes | 0 em 5 repeticoes |
| Mutex individual | mensagens do protocolo | 4 em 5 repeticoes |
| Mutex individual | espera media | 71.692 ms |
| Mutex concorrente | sobreposicoes | 0 em 15 pedidos |
| Mutex concorrente | respostas adiadas medias | 1 |
| Eleicao | deteccao de parada do lider | 4293.6 ms media |
| Eleicao | recuperacao do lider | 662.6 ms media |
| Eleicao | divergencia apos estabilizacao | 0 |

## Logs e visualizacao

```powershell
docker compose logs --no-color
```

Exemplo de saida:

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
src/node/                  No HTTP com Lamport, mutex e eleicao
src/resource/              Observador da secao critica
scripts/                   Smoke tests, demo e experimentos
tests/unit/                Testes unitarios
docs/results/              Resultados gerados por `scripts/run_experiments.py`
docs/references.md         Rastreabilidade academica
docs/algorithm-traceability.md  Matriz de rastreabilidade dos algoritmos
docs/decision-log.md       Decisoes relevantes
Dockerfile                 Imagem comum
docker-compose.yml         Pilha local com 4 servicos
pyproject.toml             Dependencias e config de testes
```

## Limitacoes

- Ricart-Agrawala assume participantes ativos durante a rodada.
- Falha de participante pode impedir progresso do mutex ate timeout.
- Bully assume falha por parada real de processo e atrasos locais limitados.
- Particoes de rede nao sao tratadas.
- O lider eleito nao coordena mutex.
- Eventos ficam apenas em memoria local.
- O observador `resource` nao participa da seguranca.

## Referencias

Veja `docs/references.md` para a lista de fontes academicas, documentacao oficial e declaracao de autoria.
