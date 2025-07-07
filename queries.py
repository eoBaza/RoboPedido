#Busines
def querie_business():
    return """
        select be.id as id_evento, payload, 
            jsonb_extract_path_text(payload::jsonb, 'data','id_cupom_pg') as id_cupom,
            be.evento, is_executed, dh_inclusao, dh_finalizacao, log, status_execucao 
            from busines_event be
            where dh_inclusao::date between current_date - INTERVAL '10 days' and current_date
            and status_execucao <> 'Sucesso'
        order by dh_inclusao desc
    """
def validar_busines_event(chave_payload, valor):
    """
    Valida eventos do Busines_event buscando por qualquer chave (ex: 'id_pedido_pg' ou 'id_cupom_pg')
    """
    query = """
    SELECT 
        id AS id_evento,
        status_execucao
    FROM Busines_event 
    WHERE 
        Payload LIKE %s
        AND dh_inclusao >= current_date - 30
    """
    parametro = f'%{chave_payload}":{valor}%'  
    return query, (parametro,)

# WMB
def validar_wmb_event(pedido):
    query = """
    select
        wmb.wmb_rowid,
        wmb.id_emp as Filial,
        wmb.id_pvd_multiplo as Pedido,
        wmb.wmb_cd_entrega 
        from Wmb_Pedido_Venda_ic wmb
        where 
        wmb.wmb_cd_entrega = 'S'
        and
        wmb.id_pvd_multiplo = :pedido
    
    
"""
    return query, {"pedido":pedido}
def validar_cupom_wmb_event(filial, cupom):
    query = """
    select 
    wmb.id_emp as filial,
    wmb.nr_cupom as Cupom,
    wmb.wmb_cd_entrega as subiu,
    wmb.id_pvd as pedido_filho,
    wmb.dt_cupom as data
    from wmb_cupom_ic wmb
    where id_emp = :1
    and nr_cupom = :2
"""
    return query, (filial, cupom)

#Venda_multiplo > saber se é R ou P
def tipo_pedido(pedido, filial):
    query = """
        select 
        id_emp as filial,
        id_pvd_multiplo as Pedido,
        Cd_modal_ent as Posterior_ou_Retira
        from pedido_venda_multiplo
        where
        st_sit_ped <> 99
        and id_pvd_multiplo = :pedido
        and id_emp = :filial
    """
    
    return query,{"pedido":pedido,"filial":filial}

#D0
def inserir_DO():
    return """
        insert into monitoraVendaEventoErro(
            Filial, pedido, nr_pdv, id_cupom_pg, nr_cupom, 
            status_evento, vl_total, data_inclusao, id_evento, is_sap
        )
        values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
def valida_D0():
    return """ 
        SELECT 
        filial, 
        pedido,
        nr_pdv,
        id_cupom_pg,
        nr_cupom,
        status_evento,
        id_evento,
        is_sap,
        data_inclusao 
    FROM monitoraVendaEventoErro
    WHERE NOT is_sap = 'OK'
    AND data_inclusao >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
    AND data_inclusao < DATE_ADD(CURDATE(), INTERVAL 1 DAY)
"""




                          # ATUALIZAÇÃO D0
def update_divida_D0():
    return  """
    update monitoraVendaEventoErro mvee set is_sap = 'OK' 
        where 
        filial = %s
        and id_cupom_pg = %s
    ;
"""
def update_venda_D0():
    return   """
    update monitoraVendaEventoErro mvee set is_sap = 'OK' 
    where filial = %s
    and pedido = %s;
"""
def validar_item_duplicadoD0(filial, nr_cupom):
    query = """
        SELECT count(*)
        FROM monitoraVendaEventoErro mvee 
        where filial = %s
        and nr_cupom = %s
        and not is_sap = 'OK'
        ;
    """
    return query, (filial, nr_cupom)

# LIMPEZA Business 
def limpeza_linha_erro_completa():
    return """
        DELETE FROM busines_event
    WHERE id IN (
        -- Eventos de DÍVIDA com sucesso já existente
        SELECT erro.id
        FROM busines_event erro
        JOIN (
            SELECT jsonb_extract_path_text(payload::jsonb, 'data','id_cupom_pg') AS id_cupom
            FROM busines_event
            WHERE status_execucao = 'Sucesso'
            GROUP BY 1
        ) sucesso
        ON jsonb_extract_path_text(erro.payload::jsonb, 'data','id_cupom_pg') = sucesso.id_cupom
        WHERE erro.status_execucao <> 'Sucesso'

        UNION

        -- Eventos de VENDA com sucesso já existente
        SELECT erro.id
        FROM busines_event erro
        JOIN (
            SELECT 
                CASE 
                    WHEN jsonb_typeof(payload::jsonb->'data'->'legacyData') = 'array' THEN
                        (payload::jsonb->'data'->'legacyData'->0->>'id_cupom_pg')::text
                    ELSE
                        payload::jsonb->'data'->>'id_cupom_pg'
                END AS id_cupom
            FROM busines_event
            WHERE status_execucao = 'Sucesso'
            GROUP BY 1
        ) sucesso
        ON 
            CASE 
                WHEN jsonb_typeof(erro.payload::jsonb->'data'->'legacyData') = 'array' THEN
                    (erro.payload::jsonb->'data'->'legacyData'->0->>'id_cupom_pg')::text
                ELSE
                    erro.payload::jsonb->'data'->>'id_cupom_pg'
            END = sucesso.id_cupom
        WHERE erro.status_execucao <> 'Sucesso'
    )
    """

#Consultar filiais concentrador
def consulta_filias():
    return"""
    select id_emp 
    from empresa where cd_tipo_emp = 'F' and cd_situacao = 1 order by id_emp asc
    """