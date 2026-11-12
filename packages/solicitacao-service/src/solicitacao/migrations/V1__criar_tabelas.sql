CREATE TABLE solicitacao (
    id                 VARCHAR2(36)              NOT NULL,
    cliente_id         VARCHAR2(64)              NOT NULL,
    produto_codigo     VARCHAR2(64)              NOT NULL,
    valor_solicitado   NUMBER(15, 2)             NOT NULL,
    email              VARCHAR2(254)             NOT NULL,
    token_dispositivo  VARCHAR2(512)             NOT NULL,
    status             VARCHAR2(30)              NOT NULL,
    motivo_decisao     VARCHAR2(500),
    criado_em          TIMESTAMP WITH TIME ZONE  NOT NULL,
    atualizado_em      TIMESTAMP WITH TIME ZONE  NOT NULL,
    versao             NUMBER(19)                NOT NULL,
    CONSTRAINT pk_solicitacao PRIMARY KEY (id),
    CONSTRAINT ck_solicitacao_status CHECK (status IN ('PENDENTE', 'PENDENTE_PUBLICACAO', 'APROVADA', 'NEGADA'))
);

CREATE INDEX ix_solicitacao_status ON solicitacao (status, criado_em);

-- Chaves de idempotência das mensagens consumidas. A constraint UNIQUE impede processamento duplicado.
CREATE TABLE chave_idempotencia (
    id         NUMBER GENERATED ALWAYS AS IDENTITY,
    chave      VARCHAR2(200)                         NOT NULL,
    criado_em  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_chave_idempotencia PRIMARY KEY (id),
    CONSTRAINT uk_chave_idempotencia UNIQUE (chave)
);
