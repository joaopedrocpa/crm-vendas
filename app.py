import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 7.6", layout="wide")

# --- MENSAGEM DE CARREGAMENTO ---
placeholder = st.empty()
placeholder.info("⏳ Carregando sistema...")

# --- FUNÇÕES VISUAIS E FORMATAÇÃO ---
def formatar_moeda_visual(valor):
    if pd.isna(valor) or str(valor).strip() == '': return "R$ 0,00"
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except: return str(valor)

def limpar_valor_monetario(valor):
    if pd.isna(valor): return 0.0
    s = str(valor).strip().replace('R$', '').strip()
    try:
        return float(s)
    except:
        if '.' in s and ',' in s: s = s.replace('.', '').replace(',', '.')
        elif ',' in s: s = s.replace(',', '.')
        try: return float(s)
        except: return 0.0

def formatar_documento(valor):
    if pd.isna(valor) or str(valor).strip() == '': return "-"
    doc = ''.join(filter(str.isdigit, str(valor)))
    if not doc: return "-"
    if len(doc) > 11:
        doc = doc.zfill(14)
        return f"{doc[:2]}.{doc[2:5]}.{doc[5:8]}/{doc[8:12]}-{doc[12:]}"
    else:
        doc = doc.zfill(11)
        return f"{doc[:3]}.{doc[3:6]}.{doc[6:9]}-{doc[9:]}"

# --- CONEXÃO ---
def conectar_google_sheets():
    try:
        creds_json = st.secrets["credenciais_google"]
        creds_dict = json.loads(creds_json)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open("Banco de Dados CRM")
    except Exception as e:
        st.error(f"Erro Conexão: {e}")
        return None

# --- CARREGAMENTO ---
@st.cache_data(ttl=60)
def carregar_dados_completos():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None, None
    try:
        try:
            sheet_clientes = spreadsheet.worksheet("Clientes")
            df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
        except: return None, None, None
        
        try:
            sheet_leads = spreadsheet.worksheet("Novos_Leads")
            dados_leads = sheet_leads.get_all_records()
            df_leads = pd.DataFrame(dados_leads)
        except: df_leads = pd.DataFrame() 
            
        if not df_leads.empty:
            df_leads = df_leads.astype(str)
            df_protheus = df_protheus.astype(str)
            df_clientes = pd.concat([df_protheus, df_leads], ignore_index=True)
        else:
            df_clientes = df_protheus

        if not df_clientes.empty:
            df_clientes.columns = df_clientes.columns.str.strip()
            df_clientes['ID_Cliente_CNPJ_CPF'] = df_clientes['ID_Cliente_CNPJ_CPF'].astype(str)
            if 'Total_Compras' in df_clientes.columns:
                df_clientes['Total_Compras'] = df_clientes['Total_Compras'].apply(limpar_valor_monetario)
            if 'Data_Ultima_Compra' in df_clientes.columns:
                df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')

        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
            if not df_interacoes.empty:
                if 'Valor_Proposta' in df_interacoes.columns:
                    df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].apply(limpar_valor_monetario)
                else: df_interacoes['Valor_Proposta'] = 0.0
                
                if 'Data' in df_interacoes.columns:
                    df_interacoes['Data_Obj'] = pd.to_datetime(df_interacoes['Data'], dayfirst=True, errors='coerce').dt.date
                
                if 'CNPJ_Cliente' in df_interacoes.columns:
                    mapa_nomes = dict(zip(df_clientes['ID_Cliente_CNPJ_CPF'].astype(str), df_clientes['Nome_Fantasia']))
                    df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
                    df_interacoes['Nome_Cliente'] = df_interacoes['CNPJ_Cliente'].map(mapa_nomes).fillna("Cliente Carteira")
        except:
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor', 'Valor_Proposta'])

        try:
            sheet_config = spreadsheet.worksheet("Config_Equipe")
            df_config = pd.DataFrame(sheet_config.get_all_records())
        except:
            df_config = pd.DataFrame(columns=['Usuario_Login', 'Carteiras_Visiveis'])

        return df_clientes, df_interacoes, df_config
    except Exception as e:
        st.error(f"Erro Dados: {e}")
        return None, None, None

# --- SALVAMENTO ---
def salvar_interacao_nuvem(cnpj, data_obj, tipo, resumo, vendedor, valor=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        data_str = data_obj.strftime('%d/%m/%Y')
        valor_str = f"{valor:.2f}".replace('.', ',') 
        sheet.append_row([str(cnpj), data_str, tipo, resumo, vendedor, valor_str])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro Salvar: {e}")
        return False

def salvar_novo_lead_completo(cnpj, nome, contato, telefone, vendedor, origem, primeira_acao, resumo_inicial, valor_inicial=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet_leads = spreadsheet.worksheet("Novos_Leads")
        nova_linha = [str(cnpj), nome.upper(), contato, "NOVO LEAD", telefone, "", "", "0", "", "0", "", vendedor, origem]
        sheet_leads.append_row(nova_linha)
        
        sheet_interacoes = spreadsheet.worksheet("Interacoes")
        data_str = datetime.now().strftime('%d/%m/%Y')
        valor_str = f"{valor_inicial:.2f}".replace('.', ',')
        sheet_interacoes.append_row([str(cnpj), data_str, primeira_acao, resumo_inicial, vendedor, valor_str])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro Lead: {e}")
        return False

# --- CALLBACKS ---
def processar_salvamento_lead(usuario_logado):
    nome = st.session_state["novo_nome"]
    doc = st.session_state["novo_doc"]
    origem = st.session_state["novo_origem"]
    acao = st.session_state["novo_acao"]
    contato = st.session_state["novo_contato"]
    tel = st.session_state["novo_tel"]
    resumo = st.session_state["novo_resumo"]
    val = st.session_state["novo_val"]
