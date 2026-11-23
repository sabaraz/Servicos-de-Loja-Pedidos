"""Rastreamento em memoria dos eventos de cada requisicao, por correlation_id.

Cada requisicao recebe uma lista propria de eventos (rota, validacao, regra de
negocio, SQL, transacao, resposta). A lista existe apenas enquanto o processo
esta ativo; nao ha armazenamento persistente.

O correlation_id da requisicao em andamento fica em um ContextVar. Assim,
qualquer funcao chamada durante a requisicao registra eventos sem receber o
identificador como parametro, e requisicoes concorrentes nao se misturam,
porque cada uma executa em seu proprio contexto.
"""

import contextvars
import threading

# Limite de requisicoes guardadas; as mais antigas sao descartadas para que a
# memoria nao cresca sem limite em sessoes longas ou experimentos de carga.
_MAX_REQUISICOES_GUARDADAS = 500

_traces: dict[str, list[dict]] = {}
_ordem: list[str] = []
_lock = threading.Lock()

current_cid: contextvars.ContextVar[str] = contextvars.ContextVar(
    "current_cid", default="sem-cid"
)


def iniciarRastreamentoDaRequisicao(correlation_id: str) -> None:
    """Abre a lista de eventos da requisicao e a torna a requisicao corrente."""
    with _lock:
        _traces[correlation_id] = []
        _ordem.append(correlation_id)
        if len(_ordem) > _MAX_REQUISICOES_GUARDADAS:
            mais_antigo = _ordem.pop(0)
            _traces.pop(mais_antigo, None)
    current_cid.set(correlation_id)


def registrarEventoNoRastreamentoDaRequisicaoCorrente(tipo: str, **dados) -> None:
    """Acrescenta um evento a requisicao corrente, identificada pelo ContextVar.

    Se a requisicao ja foi descartada pelo limite de memoria, o evento e
    ignorado em silencio, sem interromper o atendimento.
    """
    cid = current_cid.get()
    evento = {"tipo": tipo, **dados}
    with _lock:
        if cid in _traces:
            _traces[cid].append(evento)


def obterEventosRastreadosPorCorrelationId(correlation_id: str) -> list[dict] | None:
    """Devolve uma copia dos eventos da requisicao, ou None se ela nao existe."""
    with _lock:
        eventos = _traces.get(correlation_id)
        return list(eventos) if eventos is not None else None
