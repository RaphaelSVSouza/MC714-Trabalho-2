# MC714 - Trabalho 2

Implementação acadêmica local de sistemas distribuídos com comunicação HTTP/JSON real entre contêineres.

## Visão geral

O projeto adota uma arquitetura com três nós (`node1`, `node2`, `node3`) e um observador externo (`resource`). O estado permanece local e em memória, a comunicação usa FastAPI, Uvicorn e HTTPX, e a orquestração ocorre por Docker Compose.

Não há consenso, Paxos, Raft, replicação, persistência, autenticação, frontend, dashboard, Kubernetes, banco de dados, tolerância a partições ou coordenação por arquivos/volumes.

## Arquitetura

- `node1`, `node2` e `node3` executam o mesmo código em contêineres separados.
- `resource` é um observador externo, não um coordenador.
- A comunicação entre nós usa HTTP/JSON via FastAPI, Uvicorn e HTTPX.
- Cada nó possui estado local apenas em memória.
- Docker Compose orquestra a pilha local.

Mapa rápido:

```text
node1 <-> node2 <-> node3
   \        |        /
        HTTP/JSON
            |
        resource
```

## Algoritmos

### Lamport

O projeto trata como eventos Lamport os registros que avançam ou atualizam o relógio lógico:

1. evento local: `clock = clock + 1`
2. envio: `clock = clock + 1` e o valor vai em `logical_time`
3. recebimento: `clock = max(clock, received) + 1`

O endpoint `/events` é um log de observabilidade. Ele pode conter anotações do mesmo acontecimento e não deve ser lido como uma lista estritamente um-para-um de eventos teóricos.

A causalidade demonstrada no projeto vale para os cenários executados. Um timestamp menor não prova causalidade.

### Ricart-Agrawala

- Os estados `RELEASED`, `WANTED` e `HELD` são a modelagem local usada pelo projeto.
- A prioridade segue o par `(request_timestamp, node_id)`.
- As mensagens centrais são `MUTEX_REQUEST` e `MUTEX_REPLY`.
- O custo esperado sem falhas é `2 x (N - 1)` mensagens do protocolo, contando apenas `REQUEST` e `REPLY`.
- O timeout existe para limpeza local e teste, não para oferecer tolerância a falhas.
- O observador `resource` não concede acesso; ele apenas registra entrada, saída e sobreposição.

### Bully

- As mensagens centrais do protocolo são `ELECTION`, `ELECTION_OK` e `COORDINATOR`.
- `HEARTBEAT` e `election_id` são adaptações do projeto.
- O maior `node_id` ativo se torna líder.
- O líder não autoriza o mutex.
- A eleição depende de atrasos limitados e timeouts coerentes com a rede local.
- Não há tolerância a partições.
- Se os timeouts forem incompatíveis com a rede, o comportamento pode ficar inadequado.

Configuração usada pela pilha:

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

Serviços expostos no host:

- `node1`: `http://localhost:8001`
- `node2`: `http://localhost:8002`
- `node3`: `http://localhost:8003`
- `resource`: `http://localhost:8010`

Entre contêineres, use nomes do Compose, por exemplo `http://node2:8000`.

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

Iniciar eleição manual:

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

Uso rápido para demonstração:

```powershell
python scripts/demo.py
```

## Experimentos

```powershell
python scripts/run_experiments.py
```

Os dados brutos ficam em `docs/results/raw-results.json` e o resumo em `docs/results/experiment-summary.csv`. O README de resultados em `docs/results/README.md` descreve a execução versionada gerada pelo script.

## Resultados coletados

Resumo da execução versionada dos experimentos:

| Experimento | Medida | Resultado |
| --- | --- | --- |
| Lamport | violações | 0 em 5 repetições |
| Mutex individual | mensagens do protocolo | 4 em 5 repetições |
| Mutex individual | espera média | 71,692 ms |
| Mutex concorrente | sobreposições | 0 em 15 pedidos |
| Mutex concorrente | respostas adiadas médias | 1 |
| Eleição | detecção de parada do líder | 4293,6 ms em média |
| Eleição | recuperação do líder | 662,6 ms em média |
| Eleição | divergência após estabilização | 0 |

## Logs e visualização

```powershell
docker compose logs --no-color
```

Exemplo de saída:

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
src/node/                       Nó HTTP com Lamport, mutex e eleição
src/resource/                   Observador da seção crítica
scripts/                        Smoke tests, demo e experimentos
tests/unit/                     Testes unitários
docs/results/                   Resultados gerados por `scripts/run_experiments.py`
docs/references.md              Rastreabilidade acadêmica
docs/algorithm-traceability.md  Matriz de rastreabilidade dos algoritmos
docs/decision-log.md            Decisões relevantes
Dockerfile                      Imagem comum
docker-compose.yml              Pilha local com 4 serviços
pyproject.toml                  Dependências e configuração de testes
```

## Limitações

- Ricart-Agrawala continua dependente de participantes ativos durante a rodada.
- O timeout do mutex limpa estado local, mas não prova tolerância a falhas.
- Bully assume falha por parada real de processo e atrasos locais limitados.
- Partições de rede não são tratadas.
- Mensagens inferiores são rejeitadas mesmo se houver eleição em andamento.
- O líder eleito não coordena o mutex.
- Eventos e anotações ficam apenas em memória local.
- O observador `resource` não participa da segurança.

## Referências

Veja `docs/references.md` para a lista de fontes acadêmicas, documentação oficial e declaração de autoria.
