-- Cada linha representa um envio confirmado. A constraint UNIQUE da chave (canal:solicitacao) é a chave de
-- idempotência que impede que a mesma decisão seja enviada duas vezes pelo mesmo canal.
CREATE TABLE envio_notificacao (
    id              NUMBER GENERATED ALWAYS AS IDENTITY,
    chave           VARCHAR2(200)                                 NOT NULL,
    canal           VARCHAR2(10)                                  NOT NULL,
    solicitacao_id  VARCHAR2(36)                                  NOT NULL,
    enviado_em      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_envio_notificacao PRIMARY KEY (id),
    CONSTRAINT uk_envio_notificacao_chave UNIQUE (chave),
    CONSTRAINT ck_envio_notificacao_canal CHECK (canal IN ('EMAIL', 'PUSH'))
);
