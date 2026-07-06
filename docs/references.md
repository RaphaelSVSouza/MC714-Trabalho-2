# References

Data de acesso registrada para links oficiais e fontes acadÃªmicas: 2026-07-05.

## 1. Fontes acadÃªmicas

### 1.1 Lamport, Leslie

- ReferÃªncia bibliogrÃ¡fica: LAMPORT, Leslie. *Time, Clocks, and the Ordering of Events in a Distributed System*. Communications of the ACM, v. 21, n. 7, p. 558-565, 1978.
- DOI: 10.1145/359545.359563
- URL canÃ´nica: https://lamport.azurewebsites.net/pubs/time-clocks.pdf
- Grau de verificaÃ§Ã£o: texto integral consultado na cÃ³pia fornecida no enunciado.
- Conceitos utilizados: happened-before, eventos locais, envio e recebimento de mensagens, Clock Condition, ordem total por timestamp e desempate por processo.
- AplicaÃ§Ã£o no projeto: relÃ³gio lÃ³gico de Lamport, ordenaÃ§Ã£o causal demonstrada nÃ³s smoke tests e registro observÃ¡vel dos eventos do sistema.
- AdaptaÃ§Ãµes: o projeto define explicitamente a granularidade de evento lÃ³gico; confirmaÃ§Ãµes tÃ©cnicas de HTTP e anotaÃ§Ãµes de observabilidade nÃ£o sÃ£o tratadas como eventos Lamport independentes.
- LimitaÃ§Ãµes: `C(a) < C(b)` nÃ£o prova causalidade; a granularidade de evento depende da implementaÃ§Ã£o escolhida.
- PÃ¡ginas utilizadas: p. 559 para happened-before; p. 560 para Clock Condition, IR1 e IR2; p. 561 para ordem total; p. 562 para participaÃ§Ã£o e falhas.

### 1.2 Ricart e Agrawala

- ReferÃªncia bibliogrÃ¡fica: RICART, Glenn; AGRAWALA, Ashok K. *An Optimal Algorithm for MÃºtual Exclusion in Computer Networks*. Communications of the ACM, v. 24, n. 1, p. 9-17, 1981.
- DOI: 10.1145/358527.358537
- URL canÃ´nica: https://doi.org/10.1145/358527.358537
- Grau de verificaÃ§Ã£o: referÃªncia bibliogrÃ¡fica e intervalo de pÃ¡ginas conferidos; localizaÃ§Ã£o interna exata nÃ£o independentemente confirmada nesta auditoria.
- Conceitos utilizados: exclusÃ£o mÃºtua distribuÃ­da, prioridade por `(request_timestamp, node_id)`, pedido fixo por tentativa e resposta adiada quando ha concorrencia.
- AplicaÃ§Ã£o no projeto: implementaÃ§Ã£o do mutex distribuÃ­do, testes unitÃ¡rios, smoke test e experimento com trÃªs nÃ³s.
- AdaptaÃ§Ãµes: o projeto representa o estado local com `RELEASED`, `WANTED` e `HELD`; usa `request_id`; expÃµe o protocolo via HTTP/JSON; inclui timeout local para limpeza e teste; e usa o observador externo `resource` apenas para registrar entradas, saÃ­das e sobreposiÃ§Ãµes.
- LimitaÃ§Ãµes: o timeout local nÃ£o transforma o algoritmo em tolerante a falhas; ele apenas evita espera infinita em cenÃ¡rios de teste.
- PÃ¡ginas utilizadas: `pp. 9-17`.

### 1.3 Garcia-Molina, Hector

- ReferÃªncia bibliogrÃ¡fica: GARCIA-MOLINA, Hector. *Elections in a Distributed Computing System*. IEEE Transactions on Computers, v. C-31, n. 1, p. 48-59, 1982.
- DOI: 10.1109/TC.1982.1675885
- URL canÃ´nica: https://doi.org/10.1109/TC.1982.1675885
- Grau de verificaÃ§Ã£o: referÃªncia bibliogrÃ¡fica e intervalo de pÃ¡ginas conferidos; localizaÃ§Ã£o interna exata nÃ£o independentemente confirmada nesta auditoria.
- Conceitos utilizados: identificadores totalmente ordenados, eleiÃ§Ã£o pelo maior ID ativo, resposta de nÃ³s superiores e anÃºncio do coordenador vencedor.
- AplicaÃ§Ã£o no projeto: eleiÃ§Ã£o de lÃ­der entre `node1`, `node2` e `node3`, com parada e recuperaÃ§Ã£o demonstradas por Docker Compose.
- AdaptaÃ§Ãµes: `ELECTION_OK` nomeia a resposta do processo superior; `HEARTBEAT` e `election_id` sÃ£o adaptaÃ§Ãµes do projeto; o transporte Ã© HTTP/JSON; e os smoke tests usam polling para observar a convergÃªncia.
- LimitaÃ§Ãµes: nÃ£o hÃ¡ tolerÃ¢ncia a partiÃ§Ãµes; timeouts incompatÃ­veis com a rede podem provocar comportamento inadequado; e a identificaÃ§Ã£o local por `election_id` nÃ£o fornece identificaÃ§Ã£o global de todas as rodadas concorrentes.
- PÃ¡ginas utilizadas: `pp. 48-59`.

## 2. DocumentaÃ§Ã£o oficial das ferramentas

### Python 3.12

- URL: https://docs.python.org/3.12/library/asyncio-sync.html
- Uso no projeto: `asyncio.Lock` e `asyncio.Event`.
- ObservaÃ§Ã£o aplicada: `Lock` fornece exclusÃ£o mÃºtua para tarefas `asyncio`; `Event` coordena espera por sinalizaÃ§Ã£o; timeout em primitivas `asyncio` deve ser feito com `asyncio.wait_for()`.

- URL complementar: https://docs.python.org/3.12/library/asyncio-task.html#asyncio.wait_for
- Uso no projeto: timeout de espera por respostas do mutex e da eleiÃ§Ã£o.

### Tempo monotÃ´nico

- URL: https://docs.python.org/3.12/library/time.html#time.monotonic
- Uso no projeto: mediÃ§Ã£o de intervalos para timeout do mutex, heartbeat, timeout de lÃ­der e timeout de eleiÃ§Ã£o.
- ObservaÃ§Ã£o aplicada: `monotonic()` nÃ£o representa data absoluta; apenas diferenÃ§as entre chamadas sao significativas.

### FastAPI

- URL: https://fastapi.tiangolo.com/tutorial/first-steps/
- Uso no projeto: aplicaÃ§Ã£o ASGI, endpoints e mÃ©todos HTTP.

- URL complementar: https://fastapi.tiangolo.com/advanced/events/
- Uso no projeto: `lifespan`, inicializaÃ§Ã£o, encerramento e gerenciamento de tasks compartilhadas.

### Uvicorn

- URL: https://www.uvicorn.org/settings/
- Uso no projeto: aplicaÃ§Ã£o ASGI com `--host 0.0.0.0` e `--port 8000`.

### HTTPX

- URL: https://www.python-httpx.org/async/
- Uso no projeto: `AsyncClient`, requisiÃ§Ãµes HTTP assÃ­ncronas, fechamento do cliente e pooling de conexÃµes.

### Pydantic

- URL: https://docs.pydantic.dev/latest/concepts/models/
- Uso no projeto: `BaseModel`, `Field`, validaÃ§Ã£o, restriÃ§Ãµes e serializaÃ§Ã£o com `model_dump()`.

### Pytest

- URL: https://docs.pytest.org/en/stable/
- Uso no projeto: descoberta de testes, asserts e exceÃ§Ãµes esperadas.

### pytest-asyncio

- URL: https://pytest-asyncio.readthedocs.io/en/stable/reference/configuration.html
- Uso no projeto: `@pytest.mark.asyncio` e `asyncio_mode = "auto"`.

### Dockerfile

- URL: https://docs.docker.com/reference/dockerfile/
- Uso no projeto: `FROM`, `WORKDIR`, `COPY`, `RUN`, `ENV`, `EXPOSE` e `CMD`.

### Docker Compose

- URL: https://docs.docker.com/reference/compose-file/services/
- Uso no projeto: definiÃ§Ã£o dos quatro serviÃ§os, imagem, build, environment, ports, healthcheck e command.

## 3. DeclaraÃ§Ã£o de autoria

Os autores declaram que nÃ£o incorporaram implementaÃ§Ãµes externas dos algoritmos. O cÃ³digo foi desenvolvido para este trabalho com base nas referÃªncias acadÃªmicas e na documentaÃ§Ã£o oficial listadas acima. DependÃªncias de terceiros sÃ£o utilizadas por meio de seus pacotes e APIs oficiais.
