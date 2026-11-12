import json
from typing import Any

from notificacao.dominio import DecisaoCredito

DECISAO_JSON: dict[str, Any] = {
    "eventoId": "evt-1",
    "solicitacaoId": "sol-1",
    "clienteId": "cliente-123",
    "email": "cliente@exemplo.com.br",
    "tokenDispositivo": "token-abc",
    "produtoCodigo": "CARTAO-GOLD",
    "valorSolicitado": 1234.5,
    "decisao": "APROVADA",
    "motivo": "Cliente elegível",
    "decididoEm": "2025-01-02T03:04:06Z",
}


def decisao(**alteracoes: Any) -> DecisaoCredito:
    return DecisaoCredito.ler(json.dumps(DECISAO_JSON | alteracoes))
