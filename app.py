import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(page_title="CRM Protheus v2", layout="wide")
ARQUIVO_INTERACOES = 'historico_interacoes.csv'

# --- FUNÇÕES DE CARREGAMENTO E SALVAMENTO ---
@st.cache_data
def carregar_dados_protheus(arquivo):
    try:
        df = pd.read_csv(arquivo, sep=';', encoding='latin1')
        df['Data_Ultima_Compra'] = pd.to_datetime(df['Data_Ultima_Compra'], dayfirst=True, errors='coerce')
        if df['Total_Compras'].dtype == 'object':
            df['Total_Compras'] = df['Total_Compras'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df['Total_Compras'] = pd.to_numeric(df['Total_Compras'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Erro no CSV: {e}")
        return None

def carregar_interacoes():
    if os.path.exists(ARQUIVO_INTERACOES):
        return pd.read_csv(ARQUIVO_INTERACOES)
    else:
        return pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor'])

def salvar_interacao(cnpj, data, tipo, resumo, vendedor):
    nova_interacao = pd.DataFrame({
        'CNPJ_Cliente': [cnpj], 
        'Data': [data], 
        'Tipo': [tipo], 
        'Resumo': [resumo],
        'Vendedor': [vendedor]
    })
    
    if os.path.exists(ARQUIVO_INTERACOES):
        nova_interacao.to_csv(ARQUIVO_INTERACOES, mode='a', header=False, index=False)
    else:
        nova_interacao.to_csv(ARQUIVO_INTERACOES, mode='w', header=True, index=False)

# --- INTERFACE ---
st.sidebar.title("🚀 CRM Vendas - V2")
st.sidebar.info("Agora com registro de atividades!")

arquivo_upload = st.sidebar.file_uploader("Carregue o CSV do Protheus", type=['csv'])

if arquivo_upload is not None:
    # 1. Carrega dados do Protheus (Passado)
    df = carregar_dados_protheus(arquivo_upload)
    
    # 2. Carrega interações do CRM (Presente)
    df_interacoes = carregar_interacoes()
    
    if df is not None:
        # Prepara datas
        hoje = datetime.now()
        df['Dias_Sem_Comprar'] = (hoje - df['Data_Ultima_Compra']).dt.days

        # --- LÓGICA DE STATUS COMBINADA ---
        def calcular_status_final(linha):
            cnpj = linha['ID_Cliente_CNPJ_CPF']
            
            # Verifica se tem interação recente nesse cliente
            if not df_interacoes.empty:
                interacoes_cliente = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj]
                if not interacoes_cliente.empty:
                    # Pega a ultima interação
                    ultima_acao = interacoes_cliente.iloc[-1]
                    data_acao = pd.to_datetime(ultima_acao['Data'])
                    dias_acao = (hoje - data_acao).days
                    
                    # REGRA DO ORÇAMENTO (5 DIAS)
                    if ultima_acao['Tipo'] == 'Orçamento Enviado':
                        if dias_acao >= 5:
                            return '⚠️ FOLLOW-UP (Orçamento Vencido)'
                        else:
                            return '⏳ EM NEGOCIAÇÃO'
            
            # REGRA DA RECOMPRA (60 DIAS)
            if linha['Dias_Sem_Comprar'] >= 60:
                return '🔴 RECUPERAR (Inativo)'
            
            return '🟢 CARTEIRA ATIVA'

        df['Status_CRM'] = df.apply(calcular_status_final, axis=1)

        # --- LOGIN ---
        vendedores = df['Ultimo_Vendedor'].dropna().unique().tolist()
        vendedores.insert(0, "GESTOR")
        usuario = st.sidebar.selectbox("Usuário:", vendedores)

        # --- TELA PRINCIPAL ---
        if usuario == "GESTOR":
            st.title("📊 Visão da Diretoria")
            
            # Métricas
            col1, col2, col3 = st.columns(3)
            col1.metric("Clientes Inativos (+60d)", len(df[df['Status_CRM'] == '🔴 RECUPERAR (Inativo)']))
            col2.metric("Orçamentos Vencidos (+5d)", len(df[df['Status_CRM'] == '⚠️ FOLLOW-UP (Orçamento Vencido)']))
            
            # Total de Interações da Equipe
            total_ligacoes = len(df_interacoes) if not df_interacoes.empty else 0
            col3.metric("Atividades Registradas", total_ligacoes)
            
            st.divider()
            st.subheader("Quem precisa de atenção agora?")
            st.dataframe(df[['Nome_Fantasia', 'Ultimo_Vendedor', 'Status_CRM', 'Telefone_Contato1']], use_container_width=True)

        else:
            # VISÃO VENDEDOR
            st.title(f"Painel de Ação: {usuario}")
            
            # Filtra clientes do vendedor
            meus_clientes = df[df['Ultimo_Vendedor'] == usuario].copy()
            
            # Divide a tela: Lista à esquerda, Ação à direita
            col_lista, col_acao = st.columns([2, 1])
            
            with col_lista:
                st.subheader("Sua Carteira")
                
                # Filtros rápidos
                filtro = st.radio("Filtrar por:", ["Todos", "🔴 A Recuperar", "⚠️ Orçamentos Pendentes"], horizontal=True)
                
                if filtro == "🔴 A Recuperar":
                    meus_clientes = meus_clientes[meus_clientes['Status_CRM'] == '🔴 RECUPERAR (Inativo)']
                elif filtro == "⚠️ Orçamentos Pendentes":
                    meus_clientes = meus_clientes[meus_clientes['Status_CRM'] == '⚠️ FOLLOW-UP (Orçamento Vencido)']
                
                # Tabela selecionável
                cliente_selecionado_id = st.radio(
                    "Selecione um cliente para trabalhar:",
                    meus_clientes['ID_Cliente_CNPJ_CPF'].tolist(),
                    format_func=lambda x: meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == x]['Nome_Fantasia'].values[0]
                )

            with col_acao:
                if cliente_selecionado_id:
                    dados_cliente = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cliente_selecionado_id].iloc[0]
                    st.info(f" Trabalhando em: **{dados_cliente['Nome_Fantasia']}**")
                    st.write(f"📞 Tel: {dados_cliente['Telefone_Contato1']}")
                    st.write(f"📅 Última Compra: {dados_cliente['Data_Ultima_Compra'].strftime('%d/%m/%Y')}")
                    
                    with st.form("form_interacao"):
                        st.write("Registrar Atividade:")
                        tipo_acao = st.selectbox("O que você fez?", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada"])
                        resumo = st.text_area("Resumo da conversa:")
                        
                        enviado = st.form_submit_button("💾 Salvar Histórico")
                        
                        if enviado:
                            salvar_interacao(
                                cliente_selecionado_id, 
                                datetime.now(), 
                                tipo_acao, 
                                resumo, 
                                usuario
                            )
                            st.success("Salvo! Atualize a página (F5) para ver o novo status.")

else:
    st.warning("Aguardando arquivo CSV...")