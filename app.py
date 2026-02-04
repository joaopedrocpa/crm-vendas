import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json # <--- Importante para ler o novo formato

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Protheus (Nuvem)", layout="wide")

# --- CONEXÃO COM GOOGLE SHEETS ---
def conectar_google_sheets():
    try:
        # Pega as credenciais que salvamos como TEXTO (o truque das aspas)
        creds_json = st.secrets["credenciais_google"]
        # Converte o texto para o formato que o Google entende (Dicionário)
        creds_dict = json.loads(creds_json)
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # Abre a planilha
        sheet = client.open("Banco de Dados CRM").worksheet("Interacoes")
        return sheet
    except Exception as e:
        st.error(f"Erro na Conexão: {e}")
        return None

# --- FUNÇÕES ---
@st.cache_data(ttl=60)
def carregar_interacoes():
    try:
        sheet = conectar_google_sheets()
        if sheet is None: return pd.DataFrame()
        
        dados = sheet.get_all_records()
        if not dados:
            return pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor'])
        return pd.DataFrame(dados)
    except:
        return pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor'])

def salvar_interacao_nuvem(cnpj, data, tipo, resumo, vendedor):
    try:
        sheet = conectar_google_sheets()
        if sheet is None: return False
        
        sheet.append_row([str(cnpj), str(data), tipo, resumo, vendedor])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

@st.cache_data
def carregar_protheus(arquivo):
    try:
        df = pd.read_csv(arquivo, sep=';', encoding='latin1')
        df['Data_Ultima_Compra'] = pd.to_datetime(df['Data_Ultima_Compra'], dayfirst=True, errors='coerce')
        if df['Total_Compras'].dtype == 'object':
            df['Total_Compras'] = df['Total_Compras'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df['Total_Compras'] = pd.to_numeric(df['Total_Compras'], errors='coerce')
        return df
    except:
        return None

# --- INTERFACE ---
st.sidebar.title("☁️ CRM Conectado v3")
st.sidebar.success("Banco de Dados: Google Sheets")

arquivo_upload = st.sidebar.file_uploader("Carregue o CSV do Protheus", type=['csv'])

if arquivo_upload is not None:
    df = carregar_protheus(arquivo_upload)
    df_interacoes = carregar_interacoes()
    
    if df is not None:
        hoje = datetime.now()
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
                    except: pass
            
            if linha['Dias_Sem_Comprar'] >= 60:
                return '🔴 RECUPERAR'
            return '🟢 ATIVO'

        df['Status'] = df.apply(calcular_status, axis=1)

        # --- NAVEGAÇÃO ---
        vendedores = df['Ultimo_Vendedor'].dropna().unique().tolist()
        vendedores.insert(0, "GESTOR")
        usuario = st.sidebar.selectbox("Quem é você?", vendedores)

        if usuario == "GESTOR":
            st.title("Painel Diretoria")
            col1, col2, col3 = st.columns(3)
            col1.metric("A Recuperar", len(df[df['Status']=='🔴 RECUPERAR']))
            col2.metric("Follow-Ups Atrasados", len(df[df['Status']=='⚠️ FOLLOW-UP']))
            col3.metric("Atividades Hoje", len(df_interacoes))
            
            st.dataframe(df_interacoes, use_container_width=True)
            
        else:
            st.title(f"Vendedor: {usuario}")
            meus = df[df['Ultimo_Vendedor'] == usuario].copy()
            
            col_esq, col_dir = st.columns([1, 1])
            with col_esq:
                st.subheader("Lista de Trabalho")
                atencao = meus[meus['Status'].isin(['🔴 RECUPERAR', '⚠️ FOLLOW-UP'])]
                
                if atencao.empty:
                    st.success("Nada pendente!")
                else:
                    cliente_id = st.radio("Selecione:", atencao['ID_Cliente_CNPJ_CPF'].tolist(), 
                                         format_func=lambda x: atencao[atencao['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0])
            
            with col_dir:
                if 'cliente_id' in locals() and cliente_id:
                    dados = meus[meus['ID_Cliente_CNPJ_CPF'] == cliente_id].iloc[0]
                    st.info(f"Cliente: {dados['Nome_Fantasia']}")
                    st.write(f"Tel: {dados['Telefone_Contato1']}")
                    
                    with st.form("acao"):
                        tipo = st.selectbox("Ação", ["Ligação", "WhatsApp", "Orçamento Enviado", "Venda Fechada"])
                        obs = st.text_area("Obs:")
                        if st.form_submit_button("Salvar na Nuvem"):
                            if salvar_interacao_nuvem(cliente_id, datetime.now(), tipo, obs, usuario):
                                st.success("Salvo no Google Sheets!")
                                st.rerun()
