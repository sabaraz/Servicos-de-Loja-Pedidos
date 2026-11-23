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

*(em construção)*

## 6. Teste funcional

*(em construção)*

## 7. Experimentos

*(em construção)*

## 8. Convenção de nomes

O código foi escrito para ser auditado pela leitura na IDE, sem precisar executá-lo.

- **Funções e métodos:** `camelCase` com nomes longos que revelam a intenção (*Intention-Revealing Names*), por exemplo `calcularValorTotalDoPedidoSomandoItens`. É a única exceção deliberada à PEP 8.
- **Todo o resto segue a PEP 8:** variáveis, parâmetros e módulos em `snake_case`; classes em `PascalCase`; constantes em `MAIUSCULAS`.
- **Contratos externos não mudam:** campos JSON, colunas SQL, cabeçalhos HTTP e campos de log.
- **Nomes impostos pelo Python ou por bibliotecas são mantidos:** `__init__`, `setUp`, `tearDown`, métodos de bibliotecas.
- Métodos de teste começam com `test` para serem encontrados pelo `unittest`, por exemplo `testCadastrarProdutoComNomeRepetidoDevolveConflito409`.

Linters de nomes (por exemplo, a regra N802 do ruff ou `invalid-name` do pylint) acusam as funções em `camelCase`. Esses alertas são esperados por esta convenção.

## 9. Segurança e dados

- Não há senhas, tokens ou chaves no repositório.
- Todos os dados são sintéticos de laboratório.
- O banco SQLite é gerado na inicialização e não é versionado.
