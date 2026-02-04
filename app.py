import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 2.0", layout="wide")

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

# --- CARREGAMENTO DE DADOS ---
@st.cache_data(ttl=60)
def carregar_dados_completos():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None, None
    
    try:
        # 1. Carrega Clientes (Protheus)
        sheet_clientes = spreadsheet.worksheet("Clientes")
        df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
        
        # 2. Carrega Leads Manuais (Novos)
        try:
            sheet_leads = spreadsheet.worksheet("Novos_Leads")
            dados_leads = sheet_leads.get_all_records()
            df_leads = pd.DataFrame(dados_leads)
        except:
            df_leads = pd.DataFrame() 
            
        # 3. Junta as duas bases
        if not df_leads.empty:
            # Força as colunas a serem strings para evitar conflito
            df_geral = pd.concat([df_protheus, df_leads], ignore_index=True)
        else:
            df_geral = df_protheus

        # Tratamento de Moeda e Data
        if not df_geral.empty:
            if df_geral['Total_Compras'].dtype == 'object':
                df_geral['Total_Compras'] = df_geral['Total_Compras'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_geral['Total_Compras'] = pd.to_numeric(df_geral['Total_Compras'], errors='coerce')
            
            df_geral['Data_Ultima_Compra'] = pd.to_datetime(df_geral['Data_Ultima_Compra'], dayfirst=True, errors='coerce')

        # 4. Carrega Interações
        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
        except:
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor'])

        # 5. Carrega Configuração de Equipe
        try:
            sheet_config = spreadsheet.worksheet("Config_Equipe")
            df_config = pd.DataFrame(sheet_config.get_all_records())
        except:
            df_config = pd.DataFrame(columns=['Usuario_Login', 'Carteiras_Visiveis'])

        return df_geral, df_interacoes, df_config

    except Exception as e:
        st.error(f"Erro ao ler dados: {e}")
        return None, None, None

def salvar_interacao_nuvem(cnpj, data, tipo, resumo, vendedor):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        sheet.append_row([str(cnpj), str(data), tipo, resumo, vendedor])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# NOVA FUNÇÃO: SALVAR LEAD DIRETO NO GOOGLE
def salvar_novo_lead(cnpj, nome, contato, telefone, vendedor):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Novos_Leads")
        
        # Prepara a linha seguindo a ordem do seu CSV original para não quebrar
        # Ordem assumida: ID, Nome, Contato, Tipo, Tel1, Tel2, Email, Total, Data, Notas, Dias, Vendedor
        nova_linha = [
            str(cnpj),                  # ID_Cliente_CNPJ_CPF
            nome.upper(),               # Nome_Fantasia
            contato,                    # Contato
            "NOVO LEAD",                # Tipo_Cliente
            telefone,                   # Telefone_Contato1
            "",                         # Telefone_Contato2
            "",                         # Email
            "0",                        # Total_Compras
            "",                         # Data_Ultima_Compra (Vazio)
            "0",                        # Total_Notas
            "",                         # Dias_Sem_Comprar (Vazio)
            vendedor                    # Ultimo_Vendedor
        ]
        
        sheet.append_row(nova_linha)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao criar lead: {e}")
        return False

# --- INTERFACE ---
st.sidebar.title("🚀 CRM Master 2.0")

# Carrega tudo
df, df_interacoes, df_config = carregar_dados_completos()

if df is not None and not df.empty:
    hoje = datetime.now()
    df['Dias_Sem_Comprar'] = (hoje - df['Data_Ultima_Compra']).dt.days

    # Lógica de Status
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
                    if ultima['Tipo'] == 'Venda Fechada': return '⭐ VENDA RECENTE'
                    if ultima['Tipo'] == 'Ligação Realizada': return '📞 CONTATADO RECENTEMENTE'
                except: pass
        
        if pd.isna(linha['Dias_Sem_Comprar']): return '🆕 NOVO LEAD'
        if linha['Dias_Sem_Comprar'] >= 60: return '🔴 RECUPERAR'
        return '🟢 ATIVO'

    df['Status'] = df.apply(calcular_status, axis=1)

    # --- LOGIN ---
    if df_config.empty:
        usuarios_disponiveis = df['Ultimo_Vendedor'].unique().tolist()
        usuarios_disponiveis.insert(0, "GESTOR")
    else:
        usuarios_disponiveis = df_config['Usuario_Login'].unique().tolist()

    usuario_logado = st.sidebar.selectbox("Usuário:", usuarios_disponiveis)

    # --- NOVO RECURSO: CADASTRO DE LEAD ---
    if usuario_logado != "GESTOR":
        st.sidebar.markdown("---")
        with st.sidebar.expander("➕ Cadastrar Novo Lead"):
            with st.form("form_novo_lead"):
                novo_nome = st.text_input("Nome da Empresa/Cliente")
                novo_cnpj = st.text_input("CPF ou CNPJ (Só números)")
                novo_contato = st.text_input("Nome do Contato")
                novo_tel = st.text_input("Telefone / WhatsApp")
                
                if st.form_submit_button("Salvar Lead"):
                    if novo_nome and novo_cnpj:
                        if salvar_novo_lead(novo_cnpj, novo_nome, novo_contato, novo_tel, usuario_logado):
                            st.success("Lead Cadastrado! Ele já aparecerá na sua lista.")
                            st.rerun()
                    else:
                        st.warning("Preencha Nome e CPF/CNPJ.")

    # --- LÓGICA DE PERMISSÃO (CORRIGIDA PARA "TODOS") ---
    if usuario_logado == "GESTOR":
        meus_clientes = df
    else:
        if not df_config.empty:
            regra_usuario = df_config[df_config['Usuario_Login'] == usuario_logado]
            if not regra_usuario.empty:
                carteiras_string = regra_usuario.iloc[0]['Carteiras_Visiveis']
                
                # AQUI ESTÁ A CORREÇÃO:
                if "TODOS" in carteiras_string.upper(): # Verifica se tem a palavra TODOS
                    meus_clientes = df # Libera tudo
                else:
                    lista_permitida = [nome.strip() for nome in carteiras_string.split(',')]
                    meus_clientes = df[df['Ultimo_Vendedor'].isin(lista_permitida)]
            else:
                meus_clientes = pd.DataFrame()
        else:
            meus_clientes = df[df['Ultimo_Vendedor'] == usuario_logado]

    # --- ÁREA PRINCIPAL ---
    if usuario_logado == "GESTOR":
        st.title("Painel Diretoria")
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Total Base", len(df))
        kpi2.metric("A Recuperar", len(df[df['Status']=='🔴 RECUPERAR']))
        kpi3.metric("Novos Leads", len(df[df['Status']=='🆕 NOVO LEAD']))
        st.dataframe(df_interacoes.tail(10), use_container_width=True)
        
    else:
        st.title(f"Área: {usuario_logado}")
        
        if meus_clientes.empty:
            st.error("Nenhum cliente vinculado. Fale com o Gestor.")
        else:
            col_esq, col_dir = st.columns([1, 1])
            with col_esq:
                st.subheader("Sua Carteira")
                # Filtro padrão inclui NOVOS LEADS agora
                status_filter = st.multiselect("Filtrar:", ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '🆕 NOVO LEAD', '🟢 ATIVO'], default=['🔴 RECUPERAR', '🆕 NOVO LEAD'])
                filtro_final = meus_clientes[meus_clientes['Status'].isin(status_filter)]
                
                if filtro_final.empty:
                    st.info("Lista vazia para este filtro.")
                else:
                    cliente_id = st.radio("Selecione:", filtro_final['ID_Cliente_CNPJ_CPF'].tolist(), 
                                         format_func=lambda x: f"{filtro_final[filtro_final['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]} ({filtro_final[filtro_final['ID_Cliente_CNPJ_CPF']==x]['Status'].values[0]})")

            with col_dir:
                if 'cliente_id' in locals() and cliente_id:
                    dados = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cliente_id].iloc[0]
                    with st.container(border=True):
                        st.markdown(f"### {dados['Nome_Fantasia']}")
                        
                        # Mostra dono original apenas se for diferente do usuário logado
                        if dados['Ultimo_Vendedor'] != usuario_logado:
                            st.caption(f"Carteira: {dados['Ultimo_Vendedor']}")

                        c1, c2 = st.columns(2)
                        c1.write(f"📞 {dados['Telefone_Contato1']}")
                        c1.write(f"👤 {dados['Contato']}")
                        
                        if pd.isna(dados['Data_Ultima_Compra']):
                            c2.info("🆕 Cliente Novo")
                        else:
                            c2.write(f"📅 Última Compra: {dados['Data_Ultima_Compra'].strftime('%d/%m/%Y')}")
                        
                        st.divider()
                        with st.form("acao"):
                            tipo = st.selectbox("Ação", ["Ligação", "WhatsApp", "Orçamento", "Venda Fechada"])
                            obs = st.text_area("Obs:")
                            if st.form_submit_button("Salvar"):
                                salvar_interacao_nuvem(cliente_id, datetime.now(), tipo, obs, usuario_logado)
                                st.success("Salvo!")
                                st.rerun()

else:
    st.warning("Carregando...")
