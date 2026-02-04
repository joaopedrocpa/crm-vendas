import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Protheus (Auto)", layout="wide")

# --- CONEXÃO COM GOOGLE SHEETS ---
def conectar_google_sheets():
    try:
        creds_json = st.secrets["credenciais_google"]
        creds_dict = json.loads(creds_json)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open("Banco de Dados CRM")
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

# --- CARREGAMENTO DE DADOS (CACHE) ---
@st.cache_data(ttl=60)
def carregar_tudo():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None
    
    try:
        # 1. Carrega Clientes (Aba 'Clientes')
        sheet_clientes = spreadsheet.worksheet("Clientes")
        dados_clientes = sheet_clientes.get_all_records()
        df_clientes = pd.DataFrame(dados_clientes)
        
        # Tratamento de dados (Moeda e Data)
        if not df_clientes.empty:
            # Converte Total_Compras (trata ponto e virgula)
            if df_clientes['Total_Compras'].dtype == 'object':
                df_clientes['Total_Compras'] = df_clientes['Total_Compras'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_clientes['Total_Compras'] = pd.to_numeric(df_clientes['Total_Compras'], errors='coerce')
            
            # Converte Data
            df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')

        # 2. Carrega Interações (Aba 'Interacoes')
        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            dados_interacoes = sheet_interacoes.get_all_records()
            df_interacoes = pd.DataFrame(dados_interacoes)
        except:
            # Se a aba não existir ou estiver vazia
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor'])

        return df_clientes, df_interacoes

    except Exception as e:
        st.error(f"Erro ao ler abas: {e}")
        return None, None

def salvar_interacao_nuvem(cnpj, data, tipo, resumo, vendedor):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        sheet.append_row([str(cnpj), str(data), tipo, resumo, vendedor])
        st.cache_data.clear() # Limpa cache para atualizar na hora
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# --- INTERFACE ---
st.sidebar.title("🚀 CRM 100% Nuvem")
st.sidebar.success("Sistema Online")

# Carrega tudo automático (sem upload de CSV!)
df, df_interacoes = carregar_tudo()

if df is not None and not df.empty:
    hoje = datetime.now()
    
    # Cálculos
    df['Dias_Sem_Comprar'] = (hoje - df['Data_Ultima_Compra']).dt.days

    # --- LÓGICA DE STATUS ---
    def calcular_status(linha):
        cnpj = linha['ID_Cliente_CNPJ_CPF']
        
        if not df_interacoes.empty:
            cnpj_str = str(cnpj)
            df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
            filtro = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj_str]
            
            if not filtro.empty:
                ultima = filtro.iloc[-1]
                try:
                    data_acao = pd.to_datetime(ultima['Data'])
                    dias_acao = (hoje - data_acao).days
                    
                    if ultima['Tipo'] == 'Orçamento Enviado':
                        return '⚠️ FOLLOW-UP' if dias_acao >= 5 else '⏳ NEGOCIAÇÃO'
                    if ultima['Tipo'] == 'Venda Fechada':
                        return '⭐ VENDA RECENTE'
                    if ultima['Tipo'] == 'Ligação Realizada':
                        return '📞 CONTATADO RECENTEMENTE'
                except: pass
        
        if linha['Dias_Sem_Comprar'] >= 60:
            return '🔴 RECUPERAR'
        return '🟢 ATIVO'

    df['Status'] = df.apply(calcular_status, axis=1)

    # --- NAVEGAÇÃO ---
    vendedores = df['Ultimo_Vendedor'].dropna().unique().tolist()
    vendedores.sort()
    vendedores.insert(0, "GESTOR")
    usuario = st.sidebar.selectbox("Selecione seu Usuário:", vendedores)

    if usuario == "GESTOR":
        st.title("📊 Visão Geral da Equipe")
        
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Total Clientes", len(df))
        kpi2.metric("Clientes a Recuperar", len(df[df['Status']=='🔴 RECUPERAR']))
        kpi3.metric("Interações Totais", len(df_interacoes) if not df_interacoes.empty else 0)
        
        st.markdown("### 🕵️ Monitoramento de Atividades")
        if not df_interacoes.empty:
            st.dataframe(df_interacoes, use_container_width=True)
        else:
            st.info("Nenhuma interação registrada ainda.")
            
    else:
        st.title(f"💼 Área de Trabalho: {usuario}")
        meus = df[df['Ultimo_Vendedor'] == usuario].copy()
        
        col_esq, col_dir = st.columns([1, 1])
        with col_esq:
            st.subheader("📋 Sua Carteira")
            
            # Filtros de Status
            filtro_status = st.multiselect(
                "Filtrar Status:", 
                options=['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '🟢 ATIVO'],
                default=['🔴 RECUPERAR', '⚠️ FOLLOW-UP']
            )
            
            # Aplica filtro
            if filtro_status:
                meus_filtrados = meus[meus['Status'].isin(filtro_status)]
            else:
                meus_filtrados = meus
                
            if meus_filtrados.empty:
                st.info("Nenhum cliente neste filtro.")
            else:
                # Seletor de Cliente
                cliente_id = st.radio(
                    "Clique no cliente para atender:", 
                    meus_filtrados['ID_Cliente_CNPJ_CPF'].tolist(), 
                    format_func=lambda x: f"{meus_filtrados[meus_filtrados['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]} ({meus_filtrados[meus_filtrados['ID_Cliente_CNPJ_CPF']==x]['Status'].values[0]})"
                )
        
        with col_dir:
            if 'cliente_id' in locals() and cliente_id:
                dados = meus[meus['ID_Cliente_CNPJ_CPF'] == cliente_id].iloc[0]
                
                # Cartão do Cliente
                with st.container(border=True):
                    st.markdown(f"### 🏢 {dados['Nome_Fantasia']}")
                    st.caption(f"CNPJ: {dados['ID_Cliente_CNPJ_CPF']}")
                    
                    c1, c2 = st.columns(2)
                    c1.write(f"**Contato:** {dados['Contato']}")
                    c1.write(f"**Tel:** {dados['Telefone_Contato1']}")
                    c2.write(f"**Última Compra:** {dados['Data_Ultima_Compra'].strftime('%d/%m/%Y')}")
                    c2.write(f"**Valor Histórico:** R$ {dados['Total_Compras']:,.2f}")
                    
                    st.divider()
                    
                    # Formulário de Ação
                    st.write("📝 **Registrar Nova Atividade**")
                    with st.form("acao_vendedor"):
                        tipo = st.selectbox("O que você fez?", 
                            ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada", "Cliente Recusou"])
                        obs = st.text_area("Detalhes (O que o cliente falou?)")
                        
                        btn = st.form_submit_button("✅ Salvar Atividade", type="primary")
                        
                        if btn:
                            if salvar_interacao_nuvem(cliente_id, datetime.now(), tipo, obs, usuario):
                                st.success("Salvo! O status será atualizado.")
                                st.rerun()

else:
    st.warning("⚠️ Aguardando dados... Verifique se a aba 'Clientes' existe na planilha do Google.")
