"""Servico HTTP da loja: produtos, pedidos e itens de pedido (FastAPI + SQLite).

Alem das regras de negocio, este modulo concentra a observabilidade do
servico:
- cada requisicao tem um correlation_id, reaproveitado do cabecalho
  X-Correlation-ID enviado pelo cliente ou gerado aqui quando ausente;
- cada requisicao produz uma linha de log em JSON;
- uma falha controlada (atraso e/ou HTTP 503) pode ser injetada nas rotas de
  dominio por variaveis de ambiente, para os experimentos.
"""

import asyncio
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .camada_de_persistencia import (
    abrirConexaoQueFechaAoFimDaRequisicao,
    criarEsquemaEDadosDeExemploSeAusentes,
    executarBlocoEmTransacaoImediataComLockDeEscrita,
    executarInstrucaoSqlParametrizadaERegistrarNoRastreamento,
)
from .rastreamento_por_requisicao import (
    iniciarRastreamentoDaRequisicao,
    obterEventosRastreadosPorCorrelationId,
    registrarEventoNoRastreamentoDaRequisicaoCorrente,
)

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

NOME_DO_SERVICO = "servico_da_loja"
# Identifica o processo nos logs sem expor o nome da maquina, ja que os logs
# dos experimentos sao publicados como evidencia.
INSTANCIA_DO_SERVICO = os.environ.get("LOJA_NOME_DA_INSTANCIA", f"pid-{os.getpid()}")

# Correlation_id aceito do cliente: letras, digitos, ponto, hifen e sublinhado,
# ate 64 caracteres. Qualquer outro valor e descartado e um novo id e gerado,
# para que o cabecalho nao injete conteudo arbitrario nos logs.
PADRAO_DE_CORRELATION_ID_ACEITO = re.compile(r"[A-Za-z0-9._-]{1,64}")

# Falha injetada (experimentos). Valores padrao: nenhuma falha.
ATRASO_INJETADO_EM_MILISSEGUNDOS = int(os.environ.get("LOJA_ATRASO_INJETADO_MS", "0"))
PERCENTUAL_DE_RESPOSTAS_503_INJETADAS = float(os.environ.get("LOJA_PERCENTUAL_DE_FALHAS_503", "0"))
SEMENTE_DO_SORTEIO_DE_FALHAS = os.environ.get("LOJA_SEMENTE_DA_INJECAO")
PREFIXOS_DE_ROTAS_SUJEITAS_A_FALHA_INJETADA = ("/produtos", "/pedidos")

ATRASO_PADRAO_DA_ROTA_LENTA_EM_MILISSEGUNDOS = 2000

_sorteador_de_falhas = random.Random(SEMENTE_DO_SORTEIO_DE_FALHAS)

# ---------------------------------------------------------------------------
# Log estruturado em JSON (uma linha por evento)
# ---------------------------------------------------------------------------

CAMPOS_OPCIONAIS_DO_LOG = (
    "correlation_id", "operation", "method", "path", "status",
    "duration_ms", "outcome", "injected_fault", "error", "configuration",
)


class FormatadorDeLogEmJson(logging.Formatter):
    """Converte cada registro de log em uma linha JSON com campos fixos."""

    def format(self, record: logging.LogRecord) -> str:
        linha = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": NOME_DO_SERVICO,
            "instance": INSTANCIA_DO_SERVICO,
            "message": record.getMessage(),
        }
        for campo in CAMPOS_OPCIONAIS_DO_LOG:
            if hasattr(record, campo):
                linha[campo] = getattr(record, campo)
        return json.dumps(linha, ensure_ascii=False)


logger = logging.getLogger(NOME_DO_SERVICO)
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    _saida_do_log = logging.StreamHandler(sys.stdout)
    _saida_do_log.setFormatter(FormatadorDeLogEmJson())
    logger.addHandler(_saida_do_log)

# O log de acesso do Uvicorn repetiria, em outro formato, a linha JSON que o
# middleware ja emite para cada requisicao.
logging.getLogger("uvicorn.access").disabled = True


# ---------------------------------------------------------------------------
# Aplicacao
# ---------------------------------------------------------------------------

@asynccontextmanager
async def gerenciarCicloDeVidaCriandoBancoNaInicializacao(app: FastAPI) -> AsyncIterator[None]:
    """Cria o banco na inicializacao e registra a configuracao de falhas ativa."""
    criarEsquemaEDadosDeExemploSeAusentes()
    logger.info(
        "servico iniciado",
        extra={
            "configuration": {
                "atraso_injetado_ms": ATRASO_INJETADO_EM_MILISSEGUNDOS,
                "percentual_de_falhas_503": PERCENTUAL_DE_RESPOSTAS_503_INJETADAS,
                "semente_da_injecao": SEMENTE_DO_SORTEIO_DE_FALHAS,
            }
        },
    )
    yield


app = FastAPI(
    title="Servico da Loja",
    version="1.0",
    lifespan=gerenciarCicloDeVidaCriandoBancoNaInicializacao,
    description=(
        "Produtos, pedidos e itens de pedido. Cada resposta traz o cabecalho "
        "`X-Correlation-ID`; envie o seu proprio para correlacionar os logs do "
        "cliente com os do servico."
    ),
    openapi_tags=[
        {
            "name": "1. Fluxo de uso",
            "description": "Abrir pedido, adicionar item, conferir itens, "
                           "confirmar ou cancelar e consultar o resultado.",
        },
        {
            "name": "Consulta e manutencao",
            "description": "Cadastro, listagem, edicao e remocao fora do fluxo principal.",
        },
    ],
)


# ---------------------------------------------------------------------------
# Correlacao, falha injetada e log por requisicao
# ---------------------------------------------------------------------------

def _obterCorrelationIdRecebidoOuGerarNovo(request: Request) -> str:
    """Reaproveita o X-Correlation-ID do cliente, se valido; senao gera um UUID."""
    recebido = request.headers.get("X-Correlation-ID")
    if recebido and PADRAO_DE_CORRELATION_ID_ACEITO.fullmatch(recebido):
        return recebido
    return str(uuid.uuid4())


def _requisicaoEstaSujeitaAFalhaInjetada(request: Request) -> bool:
    """Somente as rotas de dominio recebem falha injetada; rotas tecnicas nao."""
    return request.url.path.startswith(PREFIXOS_DE_ROTAS_SUJEITAS_A_FALHA_INJETADA)


def _sortearSeRequisicaoRecebeFalha503() -> bool:
    """Sorteia, conforme o percentual configurado, se a resposta sera um 503."""
    return _sorteador_de_falhas.uniform(0, 100) < PERCENTUAL_DE_RESPOSTAS_503_INJETADAS


def _classificarResultadoPeloStatusHttp(status_http: int) -> str:
    """Traduz o status HTTP no campo outcome do log."""
    if status_http >= 500:
        return "server_error"
    if status_http >= 400:
        return "client_error"
    return "success"


def _obterNomeDaOperacaoAtendida(request: Request) -> str:
    """Usa o nome da funcao da rota; sem rota (ex.: 404, 503 injetado), usa metodo e caminho."""
    endpoint = request.scope.get("endpoint")
    return getattr(endpoint, "__name__", None) or f"{request.method} {request.url.path}"


def _registrarLinhaDeLogDaRequisicao(
    nivel: int,
    request: Request,
    correlation_id: str,
    status_http: int,
    duracao_ms: float,
    **campos_adicionais,
) -> None:
    """Emite a linha JSON da requisicao com os campos de correlacao e medida."""
    logger.log(
        nivel,
        "requisicao atendida",
        extra={
            "correlation_id": correlation_id,
            "operation": _obterNomeDaOperacaoAtendida(request),
            "method": request.method,
            "path": request.url.path,
            "status": status_http,
            "duration_ms": round(duracao_ms, 1),
            "outcome": _classificarResultadoPeloStatusHttp(status_http),
            **campos_adicionais,
        },
    )


async def _aplicarFalhaInjetadaOuSeguirParaRota(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    correlation_id: str,
) -> tuple[Response, bool]:
    """Aplica o atraso e o sorteio de 503 configurados; devolve (resposta, houve_503)."""
    if ATRASO_INJETADO_EM_MILISSEGUNDOS > 0:
        # asyncio.sleep nao bloqueia o laco de eventos: outras requisicoes
        # continuam sendo atendidas durante o atraso.
        await asyncio.sleep(ATRASO_INJETADO_EM_MILISSEGUNDOS / 1000)
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "INJECTED_DELAY", atraso_ms=ATRASO_INJETADO_EM_MILISSEGUNDOS
        )
    if _sortearSeRequisicaoRecebeFalha503():
        registrarEventoNoRastreamentoDaRequisicaoCorrente("INJECTED_FAILURE", status=503)
        resposta_503 = JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": "Servico indisponivel (falha injetada)", "correlation_id": correlation_id},
        )
        return resposta_503, True
    return await call_next(request), False


# A excecao inesperada e tratada dentro do proprio middleware (e nao em um
# exception_handler separado) porque o handler de Exception do Starlette roda
# em uma camada externa a este middleware: o cabecalho X-Correlation-ID nao
# chegaria a resposta de erro 500.
@app.middleware("http")
async def registrarLogEstruturadoECorrelacaoDeCadaRequisicao(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    correlation_id = _obterCorrelationIdRecebidoOuGerarNovo(request)
    iniciarRastreamentoDaRequisicao(correlation_id)
    registrarEventoNoRastreamentoDaRequisicaoCorrente(
        "REQUEST_RECEIVED", method=request.method, path=request.url.path
    )
    inicio = time.perf_counter()

    try:
        if _requisicaoEstaSujeitaAFalhaInjetada(request):
            response, houve_falha_injetada = await _aplicarFalhaInjetadaOuSeguirParaRota(
                request, call_next, correlation_id
            )
        else:
            response, houve_falha_injetada = await call_next(request), False
    except Exception as exc:
        duracao_ms = (time.perf_counter() - inicio) * 1000
        _registrarLinhaDeLogDaRequisicao(
            logging.ERROR, request, correlation_id, 500, duracao_ms, error=repr(exc)
        )
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "RESPONSE_SENT", status=500, duracao_ms=round(duracao_ms, 1)
        )
        response = JSONResponse(
            status_code=500,
            content={"error": "Erro interno no servidor", "correlation_id": correlation_id},
        )
        response.headers["X-Correlation-ID"] = correlation_id
        return response

    duracao_ms = (time.perf_counter() - inicio) * 1000
    campos_adicionais = {"injected_fault": True} if houve_falha_injetada else {}
    _registrarLinhaDeLogDaRequisicao(
        logging.INFO, request, correlation_id, response.status_code, duracao_ms, **campos_adicionais
    )
    registrarEventoNoRastreamentoDaRequisicaoCorrente(
        "RESPONSE_SENT", status=response.status_code, duracao_ms=round(duracao_ms, 1)
    )
    response.headers["X-Correlation-ID"] = correlation_id
    return response


@app.exception_handler(RequestValidationError)
async def registrarFalhaDeValidacaoEDevolverErro422(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Mantem o corpo 422 padrao do FastAPI e registra VALIDATION_FAILED no rastreamento."""
    registrarEventoNoRastreamentoDaRequisicaoCorrente(
        "VALIDATION_FAILED",
        erros=[
            {"campo": ".".join(str(p) for p in e["loc"]), "mensagem": e["msg"]}
            for e in exc.errors()
        ],
    )
    return await request_validation_exception_handler(request, exc)


# ---------------------------------------------------------------------------
# Rotas tecnicas (fora do dominio e ocultas da documentacao)
# ---------------------------------------------------------------------------

@app.get("/teste/lento", include_in_schema=False)
def responderComAtrasoConfiguravelParaTesteDeTempoLimite(
    atraso_ms: int = Query(default=ATRASO_PADRAO_DA_ROTA_LENTA_EM_MILISSEGUNDOS, ge=0, le=10000),
) -> dict:
    """Responde depois do atraso pedido, para o cliente exercitar seu tempo limite."""
    time.sleep(atraso_ms / 1000)
    return {"ok": True, "atraso_ms": atraso_ms}


@app.get("/traces/{correlation_id}", include_in_schema=False)
def consultarEventosRastreadosDaRequisicao(correlation_id: str) -> dict:
    """Devolve os eventos registrados durante a requisicao indicada."""
    eventos = obterEventosRastreadosPorCorrelationId(correlation_id)
    if eventos is None:
        raise HTTPException(status_code=404, detail="Rastreamento não encontrado")
    return {"correlation_id": correlation_id, "eventos": eventos}


def _calcularValorTotalDoPedidoSomandoItens(conn: sqlite3.Connection, pedido_id: int) -> float:
    """Soma quantidade x preco_unitario dos itens; 0.0 para pedido sem itens."""
    total = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
        conn,
        "SELECT COALESCE(SUM(quantidade * preco_unitario), 0) AS total "
        "FROM itens_pedido WHERE pedido_id = ?",
        (pedido_id,),
    ).fetchone()["total"]
    return round(total, 2)


# ---------------------------------------------------------------------------
# Esquemas - Produtos
# ---------------------------------------------------------------------------

class ProdutoEntrada(BaseModel):
    nome: str = Field(min_length=2, max_length=80)
    preco: float = Field(gt=0)
    estoque: int = Field(ge=0)


class Produto(ProdutoEntrada):
    id: int


# ---------------------------------------------------------------------------
# Rotas - Produtos
# ---------------------------------------------------------------------------

@app.get("/produtos", response_model=list[Produto], tags=["Consulta e manutencao"], summary="Listar produtos")
def listarTodosOsProdutos() -> list[dict]:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        rows = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
            conn, "SELECT * FROM produtos"
        ).fetchall()
        return [dict(r) for r in rows]


@app.post(
    "/produtos", response_model=Produto, status_code=status.HTTP_201_CREATED,
    tags=["Consulta e manutencao"], summary="Cadastrar produto",
)
def cadastrarProdutoComNomeUnico(entrada: ProdutoEntrada, response: Response) -> Produto:
    registrarEventoNoRastreamentoDaRequisicaoCorrente(
        "VALIDATION_OK", campos=list(ProdutoEntrada.model_fields.keys())
    )
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        try:
            cur = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn,
                "INSERT INTO produtos (nome, preco, estoque) VALUES (?, ?, ?)",
                (entrada.nome, entrada.preco, entrada.estoque),
            )
        except sqlite3.IntegrityError:
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "BUSINESS_CONFLICT", motivo="nome de produto já existe"
            )
            raise HTTPException(status_code=409, detail="Produto com esse nome já existe")

        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "BUSINESS_RULE_OK", regra="nome de produto disponível"
        )
        produto = Produto(id=cur.lastrowid, **entrada.model_dump())
        response.headers["Location"] = f"/produtos/{produto.id}"
        return produto


@app.get("/produtos/{produto_id}", response_model=Produto, tags=["Consulta e manutencao"], summary="Obter produto")
def obterProdutoPorIdentificador(produto_id: int) -> dict:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        row = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
            conn, "SELECT * FROM produtos WHERE id = ?", (produto_id,)
        ).fetchone()
        if not row:
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_NOT_FOUND", recurso="produto", id=produto_id
            )
            raise HTTPException(status_code=404, detail="Produto não encontrado")
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "RESOURCE_FOUND", recurso="produto", id=produto_id
        )
        return dict(row)


@app.put("/produtos/{produto_id}", response_model=Produto, tags=["Consulta e manutencao"], summary="Substituir produto")
def substituirDadosDeProdutoExistente(produto_id: int, entrada: ProdutoEntrada) -> Produto:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        cur = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
            conn, "SELECT id FROM produtos WHERE id = ?", (produto_id,)
        )
        if not cur.fetchone():
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_NOT_FOUND", recurso="produto", id=produto_id
            )
            raise HTTPException(status_code=404, detail="Produto não encontrado")
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "RESOURCE_FOUND", recurso="produto", id=produto_id
        )
        try:
            executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn,
                "UPDATE produtos SET nome = ?, preco = ?, estoque = ? WHERE id = ?",
                (entrada.nome, entrada.preco, entrada.estoque, produto_id),
            )
        except sqlite3.IntegrityError:
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "BUSINESS_CONFLICT", motivo="nome já usado por outro produto"
            )
            raise HTTPException(status_code=409, detail="Nome já usado por outro produto")
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "BUSINESS_RULE_OK", regra="nome de produto disponível"
        )
        return Produto(id=produto_id, **entrada.model_dump())


@app.delete(
    "/produtos/{produto_id}", status_code=status.HTTP_204_NO_CONTENT,
    tags=["Consulta e manutencao"], summary="Remover produto",
)
def removerProdutoSemPedidosAssociados(produto_id: int) -> Response:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        try:
            cur = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn, "DELETE FROM produtos WHERE id = ?", (produto_id,)
            )
        except sqlite3.IntegrityError:
            # A chave estrangeira de itens_pedido impede remover produto ja
            # usado em pedido: e conflito com o estado atual (409), nao falha
            # inesperada do servidor (500).
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "BUSINESS_CONFLICT", motivo="produto associado a pedidos"
            )
            raise HTTPException(
                status_code=409,
                detail="Produto associado a pedidos não pode ser removido",
            )
        if cur.rowcount == 0:
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_NOT_FOUND", recurso="produto", id=produto_id
            )
            raise HTTPException(status_code=404, detail="Produto não encontrado")
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "RESOURCE_FOUND", recurso="produto", id=produto_id
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Esquemas - Pedidos e itens
# ---------------------------------------------------------------------------

class ItemEntrada(BaseModel):
    produto_id: int
    quantidade: int = Field(gt=0)


class Item(ItemEntrada):
    id: int
    pedido_id: int
    preco_unitario: float
    # Campos calculados na resposta (nao persistidos): poupam o cliente de
    # cruzar produto_id para saber o nome e o subtotal do item.
    produto_nome: str
    subtotal: float


class PedidoStatusUpdate(BaseModel):
    status: str = Field(pattern="^(confirmado|cancelado)$")


class Pedido(BaseModel):
    id: int
    status: str
    criado_em: str
    # Calculado na resposta: soma de quantidade x preco_unitario dos itens.
    valor_total: float


# ---------------------------------------------------------------------------
# Rotas - Pedidos
# ---------------------------------------------------------------------------

@app.post(
    "/pedidos", response_model=Pedido, status_code=status.HTTP_201_CREATED,
    tags=["1. Fluxo de uso"], summary="1. Abrir pedido",
    responses={
        201: {"description": "Pedido novo criado."},
        200: {"description": "Idempotency-Key já usada; devolve o pedido existente."},
    },
)
def abrirPedidoRespeitandoChaveDeIdempotencia(
    response: Response, idempotency_key: Optional[str] = Header(default=None)
) -> dict:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        # Verificar a chave e criar o pedido na mesma transacao imediata impede
        # que duas requisicoes simultaneas com a mesma chave criem dois pedidos.
        with executarBlocoEmTransacaoImediataComLockDeEscrita(conn):
            if idempotency_key:
                existente = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                    conn, "SELECT * FROM pedidos WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if existente:
                    registrarEventoNoRastreamentoDaRequisicaoCorrente(
                        "BUSINESS_RULE_OK",
                        regra="idempotency_key já usada: devolve pedido existente",
                    )
                    response.status_code = status.HTTP_200_OK
                    return {
                        **dict(existente),
                        "valor_total": _calcularValorTotalDoPedidoSomandoItens(conn, existente["id"]),
                    }

            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "BUSINESS_RULE_OK", regra="idempotency_key nova (ou ausente): cria pedido"
            )
            agora = datetime.now(timezone.utc).isoformat()
            cur = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn,
                "INSERT INTO pedidos (status, idempotency_key, criado_em) VALUES (?, ?, ?)",
                ("aberto", idempotency_key, agora),
            )
            resultado = {"id": cur.lastrowid, "status": "aberto", "criado_em": agora, "valor_total": 0.0}

        response.headers["Location"] = f"/pedidos/{resultado['id']}"
        return resultado


@app.get("/pedidos", response_model=list[Pedido], tags=["Consulta e manutencao"], summary="Listar pedidos")
def listarTodosOsPedidosComValorTotal() -> list[dict]:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        rows = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
            conn, "SELECT * FROM pedidos"
        ).fetchall()
        return [
            {**dict(r), "valor_total": _calcularValorTotalDoPedidoSomandoItens(conn, r["id"])}
            for r in rows
        ]


@app.post(
    "/pedidos/{pedido_id}/itens", response_model=Item,
    tags=["1. Fluxo de uso"], summary="2. Adicionar item ao pedido",
    responses={
        201: {"description": "Item novo criado neste pedido."},
        200: {"description": "Produto já estava no pedido; quantidade atualizada."},
    },
)
def adicionarItemAoPedidoAbertoOuSomarQuantidade(
    pedido_id: int, entrada: ItemEntrada, response: Response
) -> dict:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        with executarBlocoEmTransacaoImediataComLockDeEscrita(conn):
            pedido = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn, "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
            ).fetchone()
            if not pedido:
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "RESOURCE_NOT_FOUND", recurso="pedido", id=pedido_id
                )
                raise HTTPException(status_code=404, detail="Pedido não encontrado")
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_FOUND", recurso="pedido", id=pedido_id
            )

            if pedido["status"] != "aberto":
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "BUSINESS_CONFLICT", motivo="pedido não está aberto"
                )
                raise HTTPException(status_code=409, detail="Pedido não está aberto para alterações")

            produto = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn, "SELECT * FROM produtos WHERE id = ?", (entrada.produto_id,)
            ).fetchone()
            if not produto:
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "RESOURCE_NOT_FOUND", recurso="produto", id=entrada.produto_id
                )
                raise HTTPException(status_code=404, detail="Produto não encontrado")
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_FOUND", recurso="produto", id=entrada.produto_id
            )
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "BUSINESS_RULE_OK", regra="pedido aberto e produto existente"
            )

            existente = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn,
                "SELECT * FROM itens_pedido WHERE pedido_id = ? AND produto_id = ?",
                (pedido_id, entrada.produto_id),
            ).fetchone()

            if existente:
                # Produto ja presente no pedido: a quantidade do item existente
                # e somada e a resposta e 200. Item novo responde 201.
                nova_qtd = existente["quantidade"] + entrada.quantidade
                executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                    conn,
                    "UPDATE itens_pedido SET quantidade = ? WHERE id = ?",
                    (nova_qtd, existente["id"]),
                )
                item_id = existente["id"]
                qtd_final = nova_qtd
                response.status_code = status.HTTP_200_OK
            else:
                cur = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                    conn,
                    "INSERT INTO itens_pedido (pedido_id, produto_id, quantidade, preco_unitario) "
                    "VALUES (?, ?, ?, ?)",
                    (pedido_id, entrada.produto_id, entrada.quantidade, produto["preco"]),
                )
                item_id = cur.lastrowid
                qtd_final = entrada.quantidade
                response.status_code = status.HTTP_201_CREATED

            resultado = {
                "id": item_id,
                "pedido_id": pedido_id,
                "produto_id": entrada.produto_id,
                "quantidade": qtd_final,
                "preco_unitario": produto["preco"],
                "produto_nome": produto["nome"],
                "subtotal": round(produto["preco"] * qtd_final, 2),
            }
        return resultado


@app.get(
    "/pedidos/{pedido_id}/itens", response_model=list[Item],
    tags=["1. Fluxo de uso"], summary="3. Conferir itens do pedido",
)
def listarItensDoPedidoComSubtotal(pedido_id: int) -> list[dict]:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        if not executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
            conn, "SELECT id FROM pedidos WHERE id = ?", (pedido_id,)
        ).fetchone():
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_NOT_FOUND", recurso="pedido", id=pedido_id
            )
            raise HTTPException(status_code=404, detail="Pedido não encontrado")
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "RESOURCE_FOUND", recurso="pedido", id=pedido_id
        )
        rows = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
            conn,
            """
            SELECT ip.*, p.nome AS produto_nome
            FROM itens_pedido ip
            JOIN produtos p ON p.id = ip.produto_id
            WHERE ip.pedido_id = ?
            """,
            (pedido_id,),
        ).fetchall()
        return [
            {**dict(r), "subtotal": round(r["preco_unitario"] * r["quantidade"], 2)}
            for r in rows
        ]


@app.patch(
    "/pedidos/{pedido_id}", response_model=Pedido,
    tags=["1. Fluxo de uso"], summary="4. Confirmar ou cancelar pedido",
    description="Ao confirmar, o estoque dos itens do pedido é reduzido nesta operação — "
                "não na adição de itens.",
)
def confirmarOuCancelarPedidoAjustandoEstoque(pedido_id: int, entrada: PedidoStatusUpdate) -> dict:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    # O lock de escrita e obtido antes de ler o status do pedido. Uma segunda
    # confirmacao simultanea so le o pedido depois do COMMIT da primeira, ja
    # como "confirmado", e recebe 409 sem baixar o estoque de novo.
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        with executarBlocoEmTransacaoImediataComLockDeEscrita(conn):
            pedido = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn, "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
            ).fetchone()
            if not pedido:
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "RESOURCE_NOT_FOUND", recurso="pedido", id=pedido_id
                )
                raise HTTPException(status_code=404, detail="Pedido não encontrado")
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_FOUND", recurso="pedido", id=pedido_id
            )

            atual = pedido["status"]
            novo = entrada.status

            if atual == "cancelado":
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "BUSINESS_CONFLICT", motivo="pedido já cancelado"
                )
                raise HTTPException(status_code=409, detail="Pedido já cancelado")
            if atual == "confirmado" and novo == "confirmado":
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "BUSINESS_CONFLICT", motivo="pedido já confirmado"
                )
                raise HTTPException(status_code=409, detail="Pedido já confirmado")

            itens = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn, "SELECT * FROM itens_pedido WHERE pedido_id = ?", (pedido_id,)
            ).fetchall()

            if atual == "aberto" and novo == "confirmado":
                if not itens:
                    registrarEventoNoRastreamentoDaRequisicaoCorrente(
                        "BUSINESS_CONFLICT", motivo="pedido sem itens"
                    )
                    raise HTTPException(status_code=409, detail="Pedido sem itens não pode ser confirmado")
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "BUSINESS_RULE_OK", regra="pedido aberto com itens: pode confirmar"
                )
                for item in itens:
                    cur = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                        conn,
                        "UPDATE produtos SET estoque = estoque - ? "
                        "WHERE id = ? AND estoque >= ?",
                        (item["quantidade"], item["produto_id"], item["quantidade"]),
                    )
                    if cur.rowcount == 0:
                        # A excecao faz a transacao imediata executar ROLLBACK,
                        # desfazendo tambem as baixas dos itens anteriores.
                        registrarEventoNoRastreamentoDaRequisicaoCorrente(
                            "BUSINESS_CONFLICT",
                            motivo=f"estoque insuficiente para produto {item['produto_id']}",
                        )
                        raise HTTPException(
                            status_code=409,
                            detail=f"Estoque insuficiente para produto {item['produto_id']}",
                        )
                executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                    conn, "UPDATE pedidos SET status = 'confirmado' WHERE id = ?", (pedido_id,)
                )

            elif atual == "confirmado" and novo == "cancelado":
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "BUSINESS_RULE_OK", regra="pedido confirmado: cancelar devolve estoque"
                )
                for item in itens:
                    executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                        conn,
                        "UPDATE produtos SET estoque = estoque + ? WHERE id = ?",
                        (item["quantidade"], item["produto_id"]),
                    )
                executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                    conn, "UPDATE pedidos SET status = 'cancelado' WHERE id = ?", (pedido_id,)
                )

            elif atual == "aberto" and novo == "cancelado":
                registrarEventoNoRastreamentoDaRequisicaoCorrente(
                    "BUSINESS_RULE_OK", regra="pedido aberto: cancelar não mexe em estoque"
                )
                executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                    conn, "UPDATE pedidos SET status = 'cancelado' WHERE id = ?", (pedido_id,)
                )

            pedido_atualizado = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
                conn, "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
            ).fetchone()
            valor_total = _calcularValorTotalDoPedidoSomandoItens(conn, pedido_id)
        return {**dict(pedido_atualizado), "valor_total": valor_total}


@app.get(
    "/pedidos/{pedido_id}", response_model=Pedido,
    tags=["1. Fluxo de uso"], summary="5. Consultar resultado do pedido",
)
def obterPedidoComValorTotal(pedido_id: int) -> dict:
    registrarEventoNoRastreamentoDaRequisicaoCorrente("VALIDATION_OK")
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        row = executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
            conn, "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
        ).fetchone()
        if not row:
            registrarEventoNoRastreamentoDaRequisicaoCorrente(
                "RESOURCE_NOT_FOUND", recurso="pedido", id=pedido_id
            )
            raise HTTPException(status_code=404, detail="Pedido não encontrado")
        registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "RESOURCE_FOUND", recurso="pedido", id=pedido_id
        )
        return {**dict(row), "valor_total": _calcularValorTotalDoPedidoSomandoItens(conn, pedido_id)}


# ---------------------------------------------------------------------------
# Front-end estatico: montado por ultimo para nao sobrepor as rotas da API.
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="front")
