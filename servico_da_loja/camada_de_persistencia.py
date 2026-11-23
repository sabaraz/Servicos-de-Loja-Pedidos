"""Acesso ao banco SQLite da loja.

Concentra conexao, execucao de SQL parametrizado, transacoes atomicas e
criacao do esquema. Toda instrucao executada aqui gera um evento no
rastreamento da requisicao corrente.
"""

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from . import rastreamento_por_requisicao

# LOJA_CAMINHO_DO_BANCO permite apontar para outro arquivo (ex.: banco
# temporario e descartavel do teste funcional ou de um experimento).
DB_PATH = Path(os.environ.get("LOJA_CAMINHO_DO_BANCO", Path(__file__).parent / "loja.db"))
SCHEMA_PATH = Path(__file__).parent / "esquema_e_dados_de_exemplo.sql"

# Tempo maximo, em segundos, que uma conexao espera pelo lock de escrita de
# outra transacao antes de desistir com erro.
TEMPO_LIMITE_DE_ESPERA_PELO_LOCK_EM_SEGUNDOS = 10


def _abrirConexaoSqliteComChavesEstrangeirasAtivas() -> sqlite3.Connection:
    """Abre uma conexao em modo autocommit, com linhas acessiveis por nome.

    Em autocommit cada instrucao isolada se efetiva sozinha; blocos que
    precisam ser atomicos usam executarBlocoEmTransacaoImediataComLockDeEscrita.
    """
    conn = sqlite3.connect(DB_PATH, timeout=TEMPO_LIMITE_DE_ESPERA_PELO_LOCK_EM_SEGUNDOS)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def abrirConexaoQueFechaAoFimDaRequisicao() -> Iterator[sqlite3.Connection]:
    """Entrega uma conexao e garante o fechamento ao sair do bloco with."""
    conn = _abrirConexaoSqliteComChavesEstrangeirasAtivas()
    try:
        yield conn
    finally:
        conn.close()


def executarInstrucaoSqlParametrizadaERegistrarNoRastreamento(
    conn: sqlite3.Connection, sql: str, params: tuple = ()
) -> sqlite3.Cursor:
    """Executa uma instrucao SQL e registra o evento SQL_<TIPO> correspondente.

    Os parametros nunca sao concatenados ao texto SQL, nem para exibicao: o
    evento guarda sql e parametros em campos separados. Para INSERT, UPDATE e
    DELETE o evento tambem informa as linhas afetadas.
    """
    cur = conn.execute(sql, params)
    tipo = sql.strip().split(None, 1)[0].upper()

    evento = {"sql": " ".join(sql.split()), "parametros": list(params)}
    if tipo in ("INSERT", "UPDATE", "DELETE"):
        evento["linhas_afetadas"] = cur.rowcount
    rastreamento_por_requisicao.registrarEventoNoRastreamentoDaRequisicaoCorrente(
        f"SQL_{tipo}", **evento
    )
    return cur


@contextmanager
def executarBlocoEmTransacaoImediataComLockDeEscrita(
    conn: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Executa o bloco with como uma unica transacao atomica.

    BEGIN IMMEDIATE obtem o lock de escrita antes de qualquer leitura do
    bloco. Uma requisicao concorrente so entra depois do COMMIT ou ROLLBACK
    da anterior e, portanto, ja enxerga o estado atualizado. Qualquer excecao
    dentro do bloco desfaz todas as alteracoes feitas nele.
    """
    conn.execute("BEGIN IMMEDIATE")
    rastreamento_por_requisicao.registrarEventoNoRastreamentoDaRequisicaoCorrente(
        "TRANSACTION_BEGIN"
    )
    try:
        yield conn
        conn.execute("COMMIT")
        rastreamento_por_requisicao.registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "TRANSACTION_COMMIT"
        )
    except Exception:
        conn.execute("ROLLBACK")
        rastreamento_por_requisicao.registrarEventoNoRastreamentoDaRequisicaoCorrente(
            "TRANSACTION_ROLLBACK"
        )
        raise


def criarEsquemaEDadosDeExemploSeAusentes() -> None:
    """Cria tabelas e produtos de exemplo que ainda nao existirem.

    Pode ser chamada a cada inicializacao: o script SQL e idempotente e nao
    duplica dados quando o banco ja existe.
    """
    with abrirConexaoQueFechaAoFimDaRequisicao() as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
