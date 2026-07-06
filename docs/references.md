# References

Data de acesso registrada para links oficiais e fontes acadêmicas: 2026-07-05.

## 1. Fontes acadêmicas

### 1.1 Lamport, Leslie

- Referência bibliográfica: LAMPORT, Leslie. *Time, Clocks, and the Ordering of Events in a Distributed System*. Communications of the ACM, v. 21, n. 7, p. 558-565, 1978.
- DOI: 10.1145/359545.359563
- URL canônica: https://lamport.azurewebsites.net/pubs/time-clocks.pdf
- Grau de verificação: texto integral consultado na cópia fornecida no enunciado.
- Conceitos utilizados: happened-before, eventos locais, envio e recebimento de mensagens, Clock Condition, ordem total por timestamp e desempate por processo.
- Aplicação no projeto: relógio lógico de Lamport, ordenação causal demonstrada nos smoke tests e registro observável dos eventos do sistema.
- Adaptações: o projeto define explicitamente a granularidade de evento lógico; confirmações técnicas de HTTP e anotações de observabilidade não são tratadas como eventos Lamport independentes.
- Limitações: `C(a) < C(b)` não prova causalidade; a granularidade de evento depende da implementação escolhida.
- Páginas utilizadas: p. 559 para happened-before; p. 560 para Clock Condition, IR1 e IR2; p. 561 para ordem total; p. 562 para participação e falhas.

### 1.2 Ricart e Agrawala

- Referência bibliográfica: RICART, Glenn; AGRAWALA, Ashok K. *An Optimal Algorithm for Mutual Exclusion in Computer Networks*. Communications of the ACM, v. 24, n. 1, p. 9-17, 1981.
- DOI: 10.1145/358527.358537
- URL canônica: https://doi.org/10.1145/358527.358537
- Grau de verificação: referência bibliográfica e intervalo de páginas conferidos; localização interna exata não independentemente confirmada nesta auditoria.
- Conceitos utilizados: exclusão mútua distribuída, prioridade por `(request_timestamp, node_id)`, pedido fixo por tentativa e resposta adiada quando há concorrência.
- Aplicação no projeto: implementação do mutex distribuído, testes unitários, smoke test e experimento com três nós.
- Adaptações: o projeto representa o estado local com `RELEASED`, `WANTED` e `HELD`; usa `request_id`; expõe o protocolo via HTTP/JSON; inclui timeout local para limpeza e teste; e usa o observador externo `resource` apenas para registrar entradas, saídas e sobreposições.
- Limitações: o timeout local não transforma o algoritmo em tolerante a falhas; ele apenas evita espera infinita em cenários de teste.
- Páginas utilizadas: `pp. 9-17`.

### 1.3 Garcia-Molina, Hector

- Referência bibliográfica: GARCIA-MOLINA, Hector. *Elections in a Distributed Computing System*. IEEE Transactions on Computers, v. C-31, n. 1, p. 48-59, 1982.
- DOI: 10.1109/TC.1982.1675885
- URL canônica: https://doi.org/10.1109/TC.1982.1675885
- Grau de verificação: referência bibliográfica e intervalo de páginas conferidos; localização interna exata não independentemente confirmada nesta auditoria.
- Conceitos utilizados: identificadores totalmente ordenados, eleição pelo maior ID ativo, resposta de nós superiores e anúncio do coordenador vencedor.
- Aplicação no projeto: eleição de líder entre `node1`, `node2` e `node3`, com parada e recuperação demonstradas por Docker Compose.
- Adaptações: `ELECTION_OK` nomeia a resposta do processo superior; `HEARTBEAT` e `election_id` são adaptações do projeto; o transporte é HTTP/JSON; e os smoke tests usam polling para observar a convergência.
- Limitações: não há tolerância a partições; timeouts incompatíveis com a rede podem provocar comportamento inadequado; e a identificação local por `election_id` não fornece identificação global de todas as rodadas concorrentes.
- Páginas utilizadas: `pp. 48-59`.

## 2. Documentação oficial das ferramentas

### Python 3.12

- URL: https://docs.python.org/3.12/library/asyncio-sync.html
- Uso no projeto: `asyncio.Lock` e `asyncio.Event`.
- Observação aplicada: `Lock` fornece exclusão mútua para tarefas `asyncio`; `Event` coordena espera por sinalização; timeout em primitivas `asyncio` deve ser feito com `asyncio.wait_for()`.

- URL complementar: https://docs.python.org/3.12/library/asyncio-task.html#asyncio.wait_for
- Uso no projeto: timeout de espera por respostas do mutex e da eleição.

### Tempo monotônico

- URL: https://docs.python.org/3.12/library/time.html#time.monotonic
- Uso no projeto: medição de intervalos para timeout do mutex, heartbeat, timeout de líder e timeout de eleição.
- Observação aplicada: `monotonic()` não representa data absoluta; apenas diferenças entre chamadas sao significativas.

### FastAPI

- URL: https://fastapi.tiangolo.com/tutorial/first-steps/
- Uso no projeto: aplicação ASGI, endpoints e métodos HTTP.

- URL complementar: https://fastapi.tiangolo.com/advanced/events/
- Uso no projeto: `lifespan`, inicialização, encerramento e gerenciamento de tasks compartilhadas.

### Uvicorn

- URL: https://www.uvicorn.org/settings/
- Uso no projeto: aplicação ASGI com `--host 0.0.0.0` e `--port 8000`.

### HTTPX

- URL: https://www.python-httpx.org/async/
- Uso no projeto: `AsyncClient`, requisições HTTP assíncronas, fechamento do cliente e pooling de conexões.

### Pydantic

- URL: https://docs.pydantic.dev/latest/concepts/models/
- Uso no projeto: `BaseModel`, `Field`, validação, restrições e serialização com `model_dump()`.

### Pytest

- URL: https://docs.pytest.org/en/stable/
- Uso no projeto: descoberta de testes, asserts e exceções esperadas.

### pytest-asyncio

- URL: https://pytest-asyncio.readthedocs.io/en/stable/reference/configuration.html
- Uso no projeto: `@pytest.mark.asyncio` e `asyncio_mode = "auto"`.

### Dockerfile

- URL: https://docs.docker.com/reference/dockerfile/
- Uso no projeto: `FROM`, `WORKDIR`, `COPY`, `RUN`, `ENV`, `EXPOSE` e `CMD`.

### Docker Compose

- URL: https://docs.docker.com/reference/compose-file/services/
- Uso no projeto: definição dos quatro serviços, imagem, build, environment, ports, healthcheck e command.

## 3. Declaração de autoria

Os autores declaram que não incorporaram implementações externas dos algoritmos. O código foi desenvolvido para este trabalho com base nas referências acadêmicas e na documentação oficial listadas acima. Dependências de terceiros são utilizadas por meio de seus pacotes e APIs oficiais.
