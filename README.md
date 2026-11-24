# AP9 — Testes, Observabilidade e Medição em Sistemas Distribuídos

> Documento em construção. As seções marcadas *(em construção)* serão completadas à medida que cada componente for implementado.

## 1. Objetivo

Medir latência, taxa de erro e throughput de um serviço HTTP de loja (produtos e pedidos), correlacionar logs entre cliente e serviço por `correlation_id` e observar o comportamento do sistema sob falhas controladas.

## 2. Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| `servico_da_loja/` | Serviço HTTP medido (FastAPI + SQLite) |
| `testes_funcionais/` | Teste funcional do fluxo da loja (`unittest`) |
| `experimentos/` | Script de medição e uma subpasta de evidências por execução |
| `relatorio/` | Relatório técnico e sua folha de estilo |

## 3. Pré-requisitos

- Python **3.11** ou superior (testado em 3.11.9).
- `pip`.
- Windows (PowerShell) ou Linux/macOS.

## 4. Preparação (uma vez)

Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux/macOS:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 5. Execução do serviço

Com o ambiente virtual ativado, a partir da raiz do repositório:

```bash
python -m uvicorn servico_da_loja.aplicacao_web:app --host 127.0.0.1 --port 8000
```

- O servidor roda sem recarga automática, para que as medições não sejam afetadas.
- O banco `servico_da_loja/loja.db` é criado na primeira inicialização, já com três produtos de exemplo. Para recomeçar do zero, pare o serviço e apague esse arquivo.
- Tela de operação: `http://127.0.0.1:8000/`.
  - Executa o fluxo de pedido: abrir, adicionar item, confirmar ou cancelar.
  - Para cada chamada, mostra o `X-Correlation-ID` enviado, os eventos internos do serviço e a resposta.
  - Toda chamada tem tempo limite de 5 s. Sem resposta, a tela mostra a falha de transporte (tempo limite ou conexão recusada).
- Documentação interativa: `http://127.0.0.1:8000/docs`.

### 5.1 Correlação e logs

- Toda resposta traz o cabeçalho `X-Correlation-ID`.
- Se o cliente enviar esse cabeçalho com um valor válido (até 64 caracteres entre letras, dígitos, `.`, `-` e `_`), o serviço reaproveita o valor. Caso contrário, gera um UUID.
- Cada requisição gera uma linha de log em JSON na saída padrão, com os campos `timestamp`, `level`, `service`, `instance`, `correlation_id`, `operation`, `method`, `path`, `status`, `duration_ms` e `outcome`.
- As linhas de inicialização do próprio Uvicorn não são JSON. Para analisar o log, considere só as linhas que começam com `{`.
- `GET /traces/{correlation_id}` devolve os eventos internos (validação, SQL, transação) de uma requisição recente.

Exemplo de verificação manual:

```bash
curl -i http://127.0.0.1:8000/produtos -H "X-Correlation-ID: teste-manual-001"
```

### 5.2 Falha injetada (experimentos)

Variáveis de ambiente lidas na inicialização. Sem elas, não há falha injetada. Elas afetam só as rotas `/produtos` e `/pedidos`.

| Variável | Efeito | Padrão |
|---|---|---|
| `LOJA_ATRASO_INJETADO_MS` | Atraso, em milissegundos, antes de cada resposta | `0` |
| `LOJA_PERCENTUAL_DE_FALHAS_503` | Percentual de respostas `503` sorteadas | `0` |
| `LOJA_SEMENTE_DA_INJECAO` | Semente do sorteio, para repetir a mesma sequência de falhas | aleatória |
| `LOJA_NOME_DA_INSTANCIA` | Valor do campo `instance` no log | `pid-<número do processo>` |
| `LOJA_CAMINHO_DO_BANCO` | Arquivo SQLite usado pelo serviço | `servico_da_loja/loja.db` |

Exemplo no PowerShell (atraso de 200 ms e 30% de falhas, sequência reproduzível):

```powershell
$env:LOJA_ATRASO_INJETADO_MS = "200"; $env:LOJA_PERCENTUAL_DE_FALHAS_503 = "30"; $env:LOJA_SEMENTE_DA_INJECAO = "42"
python -m uvicorn servico_da_loja.aplicacao_web:app --host 127.0.0.1 --port 8000
```

A configuração ativa é registrada na primeira linha de log (`"message": "servico iniciado"`). As respostas com falha injetada trazem `"injected_fault": true` no log.

## 6. Teste funcional

Com o ambiente virtual ativado, a partir da raiz do repositório:

```bash
python -m unittest -v
```

- Não é preciso subir o serviço antes. O teste o inicia em um processo separado, numa porta livre e com um banco temporário, e o encerra ao final. O banco `servico_da_loja/loja.db` não é tocado.
- São 29 cenários em condição nominal, cada um criando os próprios dados. Todas as rotas de produtos e pedidos são chamadas ao menos uma vez:
  - sucesso (200, 201, 204), conflito (409), recurso inexistente (404) e validação (422);
  - cadastro, consulta, substituição e remoção de produto;
  - listagem de pedidos e de itens, com valor total e subtotal;
  - idempotência sequencial e concorrente;
  - soma de quantidade do item;
  - todas as transições de pedido (confirmar, cancelar aberto, cancelar confirmado) e o efeito no estoque;
  - duas confirmações simultâneas;
  - tempo limite curto e suficiente;
  - correlação por `X-Correlation-ID`.
- Toda requisição tem tempo limite explícito e envia um `X-Correlation-ID` próprio. Em caso de falha, a mensagem mostra esse id, que localiza a mesma requisição no log do serviço.
- Para testar um serviço já em execução, defina o endereço dele antes:

```powershell
$env:LOJA_ENDERECO_BASE_DO_SERVICO = "http://127.0.0.1:8000"; python -m unittest -v
```

O teste funcional verifica **se** cada operação responde corretamente. Medir **quanto tempo** ela leva e como se comporta sob falha é papel dos experimentos (seção 7).

## 7. Experimentos

Com o ambiente virtual ativado, a partir da raiz do repositório:

```bash
python experimentos/experimento_de_latencia_e_falhas.py --cenario A-nominal
```

Cada cenário declara no script, antes de executar, a **hipótese**, a **perturbação** e o **critério de parada**:

| Cenário | Perturbação | Amostras × repetições | Duração aproximada |
|---|---|---|---|
| `A-nominal` | Nenhuma (referência) | 200 × 3 | 15 s |
| `A-nominal --reaproveitar-conexao` | Nenhuma; conexão keep-alive | 200 × 3 | 10 s |
| `B-falha-de-aplicacao-503` | 20% de respostas 503 (semente 42) | 200 × 3 | 15 s |
| `C-atraso-abaixo-do-tempo-limite` | Atraso de 300 ms (tempo limite de 1 s) | 100 × 3 | 2 min |
| `D-atraso-acima-do-tempo-limite` | Atraso de 1.200 ms (tempo limite de 1 s) | 30 × 3 | 2 min |
| `E-queda-do-servico` | Processo do serviço encerrado após 100 amostras | até 200 × 3 | 1 min |

**O que o script faz em cada repetição:**
- sobe o serviço num processo próprio, com banco temporário novo e as variáveis de falha do cenário;
- envia as requisições de aquecimento, que são descartadas;
- mede `GET /produtos` com um cliente único e sequencial. Cada requisição leva um `X-Correlation-ID` próprio e tempo limite explícito;
- no cenário `E`, derruba o serviço no meio da medição;
- interrompe a repetição se atingir o critério de parada (padrão: 20 erros de transporte seguidos);
- aguarda 2 s para o serviço concluir requisições que o cliente já abandonou, e então o encerra.

**Classificação de cada requisição** (feita do ponto de vista do cliente):

| Resultado | Tipo | Significado |
|---|---|---|
| `success` | — | Resposta HTTP 2xx |
| `application_error` | status HTTP | O serviço respondeu, mas com status de erro (4xx/5xx) |
| `transport_error` | `connect_timeout` | A conexão nem foi estabelecida a tempo: o serviço não recebeu a requisição |
| `transport_error` | `connection_error` | Conexão recusada ou interrompida |
| `transport_error` | `read_timeout` | A requisição foi enviada, mas a resposta não chegou a tempo: o serviço **pode tê-la executado** |

**Correlação automática.** Ao final, o script cruza `cliente.log` e `servico.log` pelo `correlation_id`. Para cada tipo de resultado visto pelo cliente, registra em `resultados.json` o que o serviço viu:
- quantas requisições chegaram ao serviço;
- com qual status;
- se o erro foi falha injetada;
- a mediana da duração no cliente e no serviço.

**Métricas:**
- latência das respostas de sucesso: mínimo, média, desvio padrão, p50, p95, p99 e máximo;
- contagem de cada tipo de erro;
- taxa de erro;
- throughput de sucessos (req/s).

Os percentis usam o método do **posto mais próximo**: o menor valor observado que é maior ou igual a p% das amostras.

| Parâmetro | Padrão |
|---|---|
| `--amostras` | 200 por repetição |
| `--aquecimento` | 20 requisições descartadas por repetição |
| `--repeticoes` | 3 (o serviço é reiniciado a cada repetição) |
| `--tempo-limite` | 1,0 s por requisição |
| `--reaproveitar-conexao` | Desligado: cada requisição abre uma conexão TCP nova |

Os valores da tabela são os padrões gerais. Um cenário pode definir os seus (veja a tabela de cenários), e o que for passado na linha de comando prevalece sobre os dois.

**Evidências gravadas em `experimentos/<cenário>/<início em UTC>/`.** Cada execução cria uma subpasta própria, e execuções anteriores nunca são sobrescritas.

| Arquivo | Conteúdo |
|---|---|
| `resultados.json` | Hipótese, perturbação, critério de parada, parâmetros, ambiente (SO, Python, bibliotecas, commit) e métricas por repetição e agregadas. Também traz a correlação entre cliente e serviço |
| `cliente.log` | Uma linha JSON por requisição do cliente, com `correlation_id`, `duration_ms` e `outcome` |
| `servico.log` | Saída do serviço, com uma linha JSON por requisição |

O mesmo `correlation_id` aparece nos dois logs. Assim, dá para separar o tempo gasto dentro do serviço (`duration_ms` do `servico.log`) do tempo total visto pelo cliente (`duration_ms` do `cliente.log`).

## 8. Relatório técnico

O relatório segue as dez seções do modelo de relatório técnico da disciplina.

| Arquivo | Papel |
|---|---|
| `relatorio/relatorio.html` | Fonte do relatório, escrita diretamente em HTML |
| `relatorio/estilo/relatorio_estilo.css` | Folha de estilo (A4, tipografia, tabelas, blocos de código) |
| `relatorio/relatorio_AP9.pdf` | Versão impressa |

Para gerar o PDF de novo, use o Microsoft Edge em modo headless. No PowerShell, a partir da raiz do repositório:

```powershell
& "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" --headless --disable-gpu --no-pdf-header-footer --print-to-pdf="$PWD\relatorio\relatorio_AP9.pdf" "$PWD\relatorio\relatorio.html"
```

- Os números de página do sumário estão escritos no HTML. Se uma edição mudar a paginação, atualize-os à mão.
- Os blocos de código mostram os números de linha reais dos arquivos deste projeto.
- As tabelas de resultados reproduzem os `resultados.json` das execuções oficiais.

## 9. Convenção de nomes

O código foi escrito para ser auditado pela leitura na IDE, sem precisar executá-lo.

- **Funções e métodos:** `camelCase` com nomes longos que revelam a intenção (*Intention-Revealing Names*), por exemplo `calcularValorTotalDoPedidoSomandoItens`. É a única exceção deliberada à PEP 8.
- **Todo o resto segue a PEP 8:** variáveis, parâmetros e módulos em `snake_case`; classes em `PascalCase`; constantes em `MAIUSCULAS`.
- **Contratos externos não mudam:** campos JSON, colunas SQL, cabeçalhos HTTP e campos de log.
- **Nomes impostos pelo Python ou por bibliotecas são mantidos:** `__init__`, `setUp`, `tearDown`, métodos de bibliotecas.
- Métodos de teste começam com `test` para serem encontrados pelo `unittest`, por exemplo `testCadastrarProdutoComNomeRepetidoDevolveConflito409`.

Linters de nomes (por exemplo, a regra N802 do ruff ou `invalid-name` do pylint) acusam as funções em `camelCase`. Esses alertas são esperados por esta convenção.

## 10. Segurança e dados

- Não há senhas, tokens ou chaves no repositório.
- Todos os dados são sintéticos de laboratório.
- O banco SQLite é gerado na inicialização e não é versionado.
