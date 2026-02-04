import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 4.0 (Financeiro)", layout="wide")

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
        # 1. Clientes (Protheus)
        sheet_clientes = spreadsheet.worksheet("Clientes")
        df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
        
        # 2. Leads (Novos)
        try:
            sheet_leads = spreadsheet.worksheet("Novos_Leads")
            dados_leads = sheet_leads.get_all_records()
            df_leads = pd.DataFrame(dados_leads)
        except:
            df_leads = pd.DataFrame() 
            
        # 3. União das Bases (Protheus + Leads)
        if not df_leads.empty:
            df_clientes = pd.concat([df_protheus, df_leads], ignore_index=True)
        else:
            df_clientes = df_protheus

        # Tratamento de Numéricos nos Clientes
        if not df_clientes.empty:
            # Garante que CNPJ seja string para cruzamento
            df_clientes['ID_Cliente_CNPJ_CPF'] = df_clientes['ID_Cliente_CNPJ_CPF'].astype(str)
            
            if df_clientes['Total_Compras'].dtype == 'object':
                df_clientes['Total_Compras'] = df_clientes['Total_Compras'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_clientes['Total_Compras'] = pd.to_numeric(df_clientes['Total_Compras'], errors='coerce')
            
            df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')

        # 4. Interações (Agora com Valor)
        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
            
            # Tratamento do Valor da Proposta
            if not df_interacoes.empty and 'Valor_Proposta' in df_interacoes.columns:
                # Converte virgula para ponto se necessário
                df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_interacoes['Valor_Proposta'] = pd.to_numeric(df_interacoes['Valor_Proposta'], errors='coerce').fillna(0)
                
                # CRUZAMENTO MÁGICO: Traz o Nome do Cliente para a tabela de Interações
                # Cria um dicionário CNPJ -> Nome
                mapa_nomes = dict(zip(df_clientes['ID_Cliente_CNPJ_CPF'], df_clientes['Nome_Fantasia']))
                # Converte CNPJ da interação para string para bater com a chave
                df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
                # Cria a coluna Nome_Cliente mapeando pelo CNPJ
                df_interacoes['Nome_Cliente'] = df_interacoes['CNPJ_Cliente'].map(mapa_nomes).fillna("Cliente Não Encontrado")
                
        except Exception as e:
            st.error(f"Erro ao processar interações: {e}")
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor', 'Valor_Proposta'])

        # 5. Configuração
        try:
            sheet_config = spreadsheet.worksheet("Config_Equipe")
            df_config = pd.DataFrame(sheet_config.get_all_records())
        except:
            df_config = pd.DataFrame(columns=['Usuario_Login', 'Carteiras_Visiveis'])

        return df_clientes, df_interacoes, df_config

    except Exception as e:
        st.error(f"Erro Geral: {e}")
        return None, None, None

def salvar_interacao_nuvem(cnpj, data, tipo, resumo, vendedor, valor=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        # Formata o valor para o padrão brasileiro no Google Sheets (opcional, mas ajuda na leitura lá)
        valor_str = f"{valor:.2f}".replace('.', ',')
        sheet.append_row([str(cnpj), str(data), tipo, resumo, vendedor, valor_str])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

def salvar_novo_lead_completo(cnpj, nome, contato, telefone, vendedor, origem, primeira_acao, resumo_inicial, valor_inicial=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet_leads = spreadsheet.worksheet("Novos_Leads")
        nova_linha = [str(cnpj), nome.upper(), contato, "NOVO LEAD", telefone, "", "", "0", "", "0", "", vendedor, origem]
        sheet_leads.append_row(nova_linha)
        
        sheet_interacoes = spreadsheet.worksheet("Interacoes")
        valor_str = f"{valor_inicial:.2f}".replace('.', ',')
        sheet_interacoes.append_row([str(cnpj), str(datetime.now()), primeira_acao, resumo_inicial, vendedor, valor_str])
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao criar lead: {e}")
        return False

# --- INTERFACE ---
st.sidebar.title("🚀 CRM Master 4.0")

df, df_interacoes, df_config = carregar_dados_completos()

if df is not None and not df.empty:
    hoje = datetime.now()
    df['Dias_Sem_Comprar'] = (hoje - df['Data_Ultima_Compra']).dt.days

    # --- LÓGICA DE STATUS ---
    def calcular_status(linha):
        cnpj = linha['ID_Cliente_CNPJ_CPF']
        if not df_interacoes.empty:
            cnpj_str = str(cnpj)
            # Filtro seguro
            filtro = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj_str]
            if not filtro.empty:
                ultima = filtro.iloc[-1]
                try:
                    data_acao = pd.to_datetime(ultima['Data'])
                    dias_acao = (hoje - data_acao).days
                    
                    if ultima['Tipo'] == 'Orçamento Enviado':
                        return '⚠️ FOLLOW-UP' if dias_acao >= 5 else '⏳ NEGOCIAÇÃO'
                    if ultima['Tipo'] == 'Venda Fechada': return '⭐ VENDA RECENTE'
                    if ultima['Tipo'] == 'Venda Perdida': return '👎 VENDA PERDIDA' # Novo Status
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

    # --- CADASTRO LEAD (Agora com Valor) ---
    if usuario_logado != "GESTOR":
        st.sidebar.markdown("---")
        with st.sidebar.expander("➕ Cadastrar Novo Lead"):
            with st.form("form_novo_lead", clear_on_submit=False):
                st.write("**Dados do Cliente**")
                novo_nome = st.text_input("Nome da Empresa/Cliente")
                novo_cnpj = st.text_input("CPF ou CNPJ (Só números)")
                novo_contato = st.text_input("Nome do Contato")
                novo_tel = st.text_input("Telefone / WhatsApp")
                
                st.write("**Origem e Ação**")
                c1, c2 = st.columns(2)
                nova_origem = c1.selectbox("Origem:", ["SELECIONE...", "SZ.CHAT", "LIGAÇÃO", "PRESENCIAL", "E-MAIL", "INDICAÇÃO"])
                primeira_acao = c2.selectbox("Ação Inicial:", ["SELECIONE...", "Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"])
                
                # Se for orçamento, pede valor
                valor_inicial = 0.0
                if primeira_acao == "Orçamento Enviado":
                    valor_inicial = st.number_input("Valor do Orçamento (R$):", min_value=0.0, step=100.0)
                
                novo_resumo = st.text_area("Resumo")
                
                if st.form_submit_button("💾 SALVAR LEAD"):
                    if not novo_nome or not novo_cnpj or nova_origem == "SELECIONE..." or primeira_acao == "SELECIONE...":
                        st.error("Preencha todos os campos obrigatórios!")
                    else:
                        if salvar_novo_lead_completo(novo_cnpj, novo_nome, novo_contato, novo_tel, usuario_logado, nova_origem, primeira_acao, novo_resumo, valor_inicial):
                            st.success("Salvo com sucesso!")
                            st.rerun()

    # --- PERMISSÕES ---
    if usuario_logado == "GESTOR":
        meus_clientes = df
    else:
        if not df_config.empty:
            regra = df_config[df_config['Usuario_Login'] == usuario_logado]
            if not regra.empty:
                carteiras = regra.iloc[0]['Carteiras_Visiveis']
                if "TODOS" in carteiras.upper(): 
                    meus_clientes = df 
                else:
                    lista = [n.strip() for n in carteiras.split(',')]
                    meus_clientes = df[df['Ultimo_Vendedor'].isin(lista)]
            else:
                meus_clientes = pd.DataFrame()
        else:
            meus_clientes = df[df['Ultimo_Vendedor'] == usuario_logado]

    # --- PAINEL DO GESTOR (ATUALIZADO) ---
    if usuario_logado == "GESTOR":
        st.title("📊 Painel Financeiro e Performance")
        
        # CÁLCULOS DE KPI
        # 1. Pipeline Aberto (Orçamentos que não foram fechados nem perdidos recentemente)
        # Uma lógica simplificada: Soma de todos os orçamentos enviados nos ultimos 30 dias que ainda não viraram venda
        # Para simplificar agora: Soma de tudo que foi marcado como 'Orçamento Enviado' (Visão Bruta de Volume)
        
        df_orcamentos = df_interacoes[df_interacoes['Tipo'] == 'Orçamento Enviado']
        total_orcado = df_orcamentos['Valor_Proposta'].sum()
        qtd_orcamentos = len(df_orcamentos)
        
        df_perdidos = df_interacoes[df_interacoes['Tipo'] == 'Venda Perdida']
        total_perdido = df_perdidos['Valor_Proposta'].sum()
        
        df_fechados = df_interacoes[df_interacoes['Tipo'] == 'Venda Fechada']
        qtd_fechados = len(df_fechados)
        
        # Taxa de Conversão
        taxa_conversao = (qtd_fechados / qtd_orcamentos * 100) if qtd_orcamentos > 0 else 0

        # Mostrando os KPIs
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("💰 Total Orçado (Geral)", f"R$ {total_orcado:,.2f}", f"{qtd_orcamentos} propostas")
        c2.metric("👎 Vendas Perdidas", f"R$ {total_perdido:,.2f}", f"{len(df_perdidos)} perdidas")
        c3.metric("✅ Vendas Fechadas", f"{qtd_fechados}", f"Conv: {taxa_conversao:.1f}%")
        c4.metric("🚨 A Recuperar (+60d)", len(df[df['Status'] == '🔴 RECUPERAR']))

        st.divider()
        
        st.subheader("🕵️ Últimas Interações (Com Nomes e Valores)")
        # Mostra colunas relevantes e ordena por data
        colunas_view = ['Data', 'Nome_Cliente', 'Tipo', 'Resumo', 'Valor_Proposta', 'Vendedor']
        # Filtra apenas colunas que existem (segurança)
        colunas_finais = [c for c in colunas_view if c in df_interacoes.columns]
        
        st.dataframe(
            df_interacoes[colunas_finais].sort_index(ascending=False).head(20), 
            use_container_width=True
        )
        
    # --- ÁREA DO VENDEDOR ---
    else:
        st.title(f"Área: {usuario_logado}")
        
        if meus_clientes.empty:
            st.error("Nenhum cliente vinculado.")
        else:
            col_esq, col_dir = st.columns([1, 1])
            with col_esq:
                st.subheader("Sua Carteira")
                opcoes_status = ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO', '👎 VENDA PERDIDA', '🟢 ATIVO']
                status_filter = st.multiselect("Filtrar:", opcoes_status, default=['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO'])
                
                filtro_final = meus_clientes[meus_clientes['Status'].isin(status_filter)]
                
                if filtro_final.empty:
                    st.info("Lista vazia.")
                else:
                    filtro_final = filtro_final.sort_values(by=['Status'], ascending=False)
                    cliente_id = st.radio("Selecione:", filtro_final['ID_Cliente_CNPJ_CPF'].tolist(), 
                                         format_func=lambda x: f"{filtro_final[filtro_final['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]} ({filtro_final[filtro_final['ID_Cliente_CNPJ_CPF']==x]['Status'].values[0]})")

            with col_dir:
                if 'cliente_id' in locals() and cliente_id:
                    dados = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cliente_id].iloc[0]
                    with st.container(border=True):
                        st.markdown(f"### {dados['Nome_Fantasia']}")
                        st.info(f"Status: **{dados['Status']}**")
                        
                        st.divider()
                        with st.form("acao"):
                            tipo = st.selectbox("Nova Ação", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada", "Venda Perdida"])
                            
                            # CAMPO CONDICIONAL DE VALOR
                            valor_acao = 0.0
                            if tipo == "Orçamento Enviado":
                                st.markdown("**💲 Qual o valor da proposta?**")
                                valor_acao = st.number_input("Valor (R$):", min_value=0.0, step=100.0)
                            
                            if tipo == "Venda Perdida":
                                st.markdown("**💸 Qual valor foi perdido?**")
                                valor_acao = st.number_input("Valor Estimado (R$):", min_value=0.0, step=100.0)

                            obs = st.text_area("Obs:")
                            
                            if st.form_submit_button("Salvar Histórico"):
                                salvar_interacao_nuvem(cliente_id, datetime.now(), tipo, obs, usuario_logado, valor_acao)
                                st.success("Salvo!")
                                st.rerun()
else:
    st.warning("Carregando base de dados...")
