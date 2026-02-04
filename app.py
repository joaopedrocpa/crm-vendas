import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 3.0", layout="wide")

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
            df_geral = pd.concat([df_protheus, df_leads], ignore_index=True)
        else:
            df_geral = df_protheus

        # Tratamento de dados
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

# FUNÇÃO REVISADA: SALVA LEAD + ORIGEM + PRIMEIRA INTERAÇÃO
def salvar_novo_lead_completo(cnpj, nome, contato, telefone, vendedor, origem, primeira_acao, resumo_inicial):
    try:
        spreadsheet = conectar_google_sheets()
        
        # 1. Salva na aba Novos_Leads
        sheet_leads = spreadsheet.worksheet("Novos_Leads")
        nova_linha = [
            str(cnpj), nome.upper(), contato, "NOVO LEAD", telefone, "", "", "0", "", "0", "", 
            vendedor, # Coluna Ultimo_Vendedor
            origem    # Coluna Origem (Nova)
        ]
        sheet_leads.append_row(nova_linha)
        
        # 2. Salva na aba Interacoes (Para já gerar status)
        sheet_interacoes = spreadsheet.worksheet("Interacoes")
        sheet_interacoes.append_row([str(cnpj), str(datetime.now()), primeira_acao, resumo_inicial, vendedor])
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao criar lead: {e}")
        return False

# --- INTERFACE ---
st.sidebar.title("🚀 CRM Master 3.0")

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
                    if ultima['Tipo'] == 'WhatsApp Enviado': return '💬 WHATSAPP INICIADO'
                except: pass
        
        if pd.isna(linha['Dias_Sem_Comprar']): return '🆕 NOVO S/ INTERAÇÃO'
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

    # --- CADASTRO DE LEAD BLINDADO ---
    if usuario_logado != "GESTOR":
        st.sidebar.markdown("---")
        with st.sidebar.expander("➕ Cadastrar Novo Lead"):
            with st.form("form_novo_lead", clear_on_submit=False):
                st.write("**Dados do Cliente**")
                novo_nome = st.text_input("Nome da Empresa/Cliente")
                novo_cnpj = st.text_input("CPF ou CNPJ (Só números)")
                novo_contato = st.text_input("Nome do Contato")
                novo_tel = st.text_input("Telefone / WhatsApp")
                
                st.write("**Origem e Status Inicial**")
                nova_origem = st.selectbox("Origem do Lead:", 
                    ["SELECIONE...", "SZ.CHAT", "LIGAÇÃO", "PRESENCIAL", "E-MAIL", "INDICAÇÃO"])
                
                primeira_acao = st.selectbox("Primeira Ação Realizada:", 
                    ["SELECIONE...", "Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"])
                
                novo_resumo = st.text_area("Resumo (O que foi conversado?)")
                
                # Botão de Envio
                enviado = st.form_submit_button("💾 SALVAR LEAD")
                
                if enviado:
                    # VALIDAÇÃO FORTE: Só salva se tudo estiver preenchido
                    erros = []
                    if not novo_nome: erros.append("Falta Nome")
                    if not novo_cnpj: erros.append("Falta CPF/CNPJ")
                    if nova_origem == "SELECIONE...": erros.append("Selecione a Origem")
                    if primeira_acao == "SELECIONE...": erros.append("Selecione a Primeira Ação (Status)")
                    
                    if erros:
                        st.error(f"Preencha todos os campos! {', '.join(erros)}")
                    else:
                        sucesso = salvar_novo_lead_completo(
                            novo_cnpj, novo_nome, novo_contato, novo_tel, 
                            usuario_logado, nova_origem, primeira_acao, novo_resumo
                        )
                        if sucesso:
                            st.success("Lead cadastrado e Status atualizado!")
                            st.rerun()

    # --- LÓGICA DE PERMISSÃO ---
    if usuario_logado == "GESTOR":
        meus_clientes = df
    else:
        if not df_config.empty:
            regra_usuario = df_config[df_config['Usuario_Login'] == usuario_logado]
            if not regra_usuario.empty:
                carteiras_string = regra_usuario.iloc[0]['Carteiras_Visiveis']
                if "TODOS" in carteiras_string.upper(): 
                    meus_clientes = df 
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
        
        # Filtra status corretamente ignorando erros de NaN
        filtro_recuperar = df[df['Status'] == '🔴 RECUPERAR']
        kpi2.metric("A Recuperar", len(filtro_recuperar))
        
        # Conta Leads novos baseados na origem se existir a coluna, ou pelo status
        if 'Origem' in df.columns:
             kpi3.metric("Leads Cadastrados", len(df[df['Ultimo_Vendedor'] != ''])) # Exemplo genérico
        else:
             kpi3.metric("Interações Hoje", len(df_interacoes[pd.to_datetime(df_interacoes['Data']).dt.date == hoje.date()]) if not df_interacoes.empty else 0)
             
        st.subheader("Últimas Interações")
        st.dataframe(df_interacoes.tail(15), use_container_width=True)
        
    else:
        st.title(f"Área: {usuario_logado}")
        
        if meus_clientes.empty:
            st.error("Nenhum cliente vinculado.")
        else:
            col_esq, col_dir = st.columns([1, 1])
            with col_esq:
                st.subheader("Sua Carteira")
                # Opções de filtro atualizadas
                opcoes_status = ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO', '📞 CONTATADO RECENTEMENTE', '🟢 ATIVO']
                status_filter = st.multiselect("Filtrar:", opcoes_status, default=['🔴 RECUPERAR', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO'])
                
                filtro_final = meus_clientes[meus_clientes['Status'].isin(status_filter)]
                
                if filtro_final.empty:
                    st.info("Nenhum cliente neste status.")
                else:
                    cliente_id = st.radio("Selecione:", filtro_final['ID_Cliente_CNPJ_CPF'].tolist(), 
                                         format_func=lambda x: f"{filtro_final[filtro_final['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]} ({filtro_final[filtro_final['ID_Cliente_CNPJ_CPF']==x]['Status'].values[0]})")

            with col_dir:
                if 'cliente_id' in locals() and cliente_id:
                    dados = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cliente_id].iloc[0]
                    with st.container(border=True):
                        st.markdown(f"### {dados['Nome_Fantasia']}")
                        if dados['Ultimo_Vendedor'] != usuario_logado:
                            st.caption(f"Carteira: {dados['Ultimo_Vendedor']}")

                        c1, c2 = st.columns(2)
                        c1.write(f"📞 {dados['Telefone_Contato1']}")
                        
                        # Mostra Origem se tiver
                        if 'Origem' in dados and str(dados['Origem']) != 'nan':
                            c2.write(f"📌 Origem: **{dados['Origem']}**")
                        
                        if pd.isna(dados['Data_Ultima_Compra']):
                            c2.info(f"Status: {dados['Status']}")
                        else:
                            c2.write(f"📅 Compra: {dados['Data_Ultima_Compra'].strftime('%d/%m/%Y')}")
                        
                        st.divider()
                        with st.form("acao"):
                            tipo = st.selectbox("Nova Ação", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada"])
                            obs = st.text_area("Obs:")
                            if st.form_submit_button("Salvar Histórico"):
                                salvar_interacao_nuvem(cliente_id, datetime.now(), tipo, obs, usuario_logado)
                                st.success("Salvo!")
                                st.rerun()

else:
    st.warning("Carregando base de dados...")
