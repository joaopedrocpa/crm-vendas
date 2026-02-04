import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 5.0", layout="wide")

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
        # 1. Clientes
        sheet_clientes = spreadsheet.worksheet("Clientes")
        df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
        
        # 2. Leads
        try:
            sheet_leads = spreadsheet.worksheet("Novos_Leads")
            dados_leads = sheet_leads.get_all_records()
            df_leads = pd.DataFrame(dados_leads)
        except:
            df_leads = pd.DataFrame() 
            
        # 3. Join
        if not df_leads.empty:
            df_clientes = pd.concat([df_protheus, df_leads], ignore_index=True)
        else:
            df_clientes = df_protheus

        # Tratamento
        if not df_clientes.empty:
            df_clientes['ID_Cliente_CNPJ_CPF'] = df_clientes['ID_Cliente_CNPJ_CPF'].astype(str)
            
            if df_clientes['Total_Compras'].dtype == 'object':
                df_clientes['Total_Compras'] = df_clientes['Total_Compras'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_clientes['Total_Compras'] = pd.to_numeric(df_clientes['Total_Compras'], errors='coerce')
            
            df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')

        # 4. Interações
        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
            
            if not df_interacoes.empty:
                if 'Valor_Proposta' in df_interacoes.columns:
                    df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df_interacoes['Valor_Proposta'] = pd.to_numeric(df_interacoes['Valor_Proposta'], errors='coerce').fillna(0)
                
                # Garante formato de data
                df_interacoes['Data'] = pd.to_datetime(df_interacoes['Data'], dayfirst=True, errors='coerce')
                
                # Cruzamento de Nomes
                mapa_nomes = dict(zip(df_clientes['ID_Cliente_CNPJ_CPF'], df_clientes['Nome_Fantasia']))
                df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
                df_interacoes['Nome_Cliente'] = df_interacoes['CNPJ_Cliente'].map(mapa_nomes).fillna("Desconhecido")
                
        except:
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor', 'Valor_Proposta'])

        # 5. Config
        try:
            sheet_config = spreadsheet.worksheet("Config_Equipe")
            df_config = pd.DataFrame(sheet_config.get_all_records())
        except:
            df_config = pd.DataFrame(columns=['Usuario_Login', 'Carteiras_Visiveis'])

        return df_clientes, df_interacoes, df_config

    except Exception as e:
        st.error(f"Erro ao ler dados: {e}")
        return None, None, None

def salvar_interacao_nuvem(cnpj, data, tipo, resumo, vendedor, valor=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
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
st.sidebar.title("🚀 CRM Master 5.0")

df, df_interacoes, df_config = carregar_dados_completos()

if df is not None and not df.empty:
    hoje = datetime.now()
    df['Dias_Sem_Comprar'] = (hoje - df['Data_Ultima_Compra']).dt.days

    def calcular_status(linha):
        cnpj = linha['ID_Cliente_CNPJ_CPF']
        if not df_interacoes.empty:
            cnpj_str = str(cnpj)
            filtro = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj_str]
            if not filtro.empty:
                ultima = filtro.iloc[-1]
                try:
                    data_acao = pd.to_datetime(ultima['Data']) if pd.notna(ultima['Data']) else None
                    if data_acao:
                        dias_acao = (hoje - data_acao).days
                        if ultima['Tipo'] == 'Orçamento Enviado':
                            return '⚠️ FOLLOW-UP' if dias_acao >= 5 else '⏳ NEGOCIAÇÃO'
                        if ultima['Tipo'] == 'Venda Fechada': return '⭐ VENDA RECENTE'
                        if ultima['Tipo'] == 'Venda Perdida': return '👎 VENDA PERDIDA'
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

    # --- CADASTRO LEAD (SEM FORMULÁRIO TRAVADO) ---
    if usuario_logado != "GESTOR":
        st.sidebar.markdown("---")
        with st.sidebar.expander("➕ Cadastrar Novo Lead"):
            # Removemos st.form para ter interatividade
            st.write("**Dados do Cliente**")
            novo_nome = st.text_input("Nome da Empresa/Cliente")
            novo_cnpj = st.text_input("CPF ou CNPJ (Só números)")
            novo_contato = st.text_input("Nome do Contato")
            novo_tel = st.text_input("Telefone / WhatsApp")
            
            st.write("**Origem e Ação**")
            c1, c2 = st.columns(2)
            nova_origem = c1.selectbox("Origem:", ["SELECIONE...", "SZ.CHAT", "LIGAÇÃO", "PRESENCIAL", "E-MAIL", "INDICAÇÃO"])
            primeira_acao = c2.selectbox("Ação Inicial:", ["SELECIONE...", "Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"])
            
            # Campo de Valor aparece na hora
            valor_inicial = 0.0
            if primeira_acao == "Orçamento Enviado":
                valor_inicial = st.number_input("Valor do Orçamento (R$):", min_value=0.0, step=100.0, key="vlr_lead")
            
            novo_resumo = st.text_area("Resumo")
            
            if st.button("💾 SALVAR LEAD", type="primary"):
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

    # --- PAINEL DO GESTOR (COM FILTROS E RANKING) ---
    if usuario_logado == "GESTOR":
        st.title("📊 Painel Financeiro e Performance")
        
        # --- FILTROS DE DATA E TIPO ---
        with st.container(border=True):
            col_f1, col_f2, col_f3 = st.columns(3)
            
            # Filtro de Data
            data_inicio = col_f1.date_input("De:", value=hoje.replace(day=1)) # Padrão: dia 1 do mês
            data_fim = col_f2.date_input("Até:", value=hoje)
            
            # Filtro de Tipo
            tipos_disponiveis = df_interacoes['Tipo'].unique().tolist()
            tipos_filtro = col_f3.multiselect("Filtrar Tipo:", options=tipos_disponiveis, default=tipos_disponiveis)
        
        # APLICAÇÃO DOS FILTROS
        if not df_interacoes.empty:
            # Filtra Data (Converte coluna Data para datetime64[ns] para comparar com date)
            mask_data = (df_interacoes['Data'].dt.date >= data_inicio) & (df_interacoes['Data'].dt.date <= data_fim)
            # Filtra Tipo
            mask_tipo = df_interacoes['Tipo'].isin(tipos_filtro)
            
            df_filtrado = df_interacoes[mask_data & mask_tipo]
        else:
            df_filtrado = pd.DataFrame()

        st.divider()

        # --- RANKING E KPI ---
        tab1, tab2 = st.tabs(["🏆 Ranking de Vendedores", "📝 Lista de Interações"])
        
        with tab1:
            if not df_filtrado.empty:
                # Agrupamento Inteligente
                ranking = df_filtrado.groupby('Vendedor').agg(
                    Orcamentos_Qtd=('Tipo', lambda x: (x == 'Orçamento Enviado').sum()),
                    Vendas_Fechadas_Qtd=('Tipo', lambda x: (x == 'Venda Fechada').sum()),
                    Vendas_Fechadas_Valor=('Valor_Proposta', lambda x: x[df_filtrado['Tipo'] == 'Venda Fechada'].sum()),
                    Vendas_Perdidas_Valor=('Valor_Proposta', lambda x: x[df_filtrado['Tipo'] == 'Venda Perdida'].sum())
                ).reset_index()
                
                # Ordena por Faturamento
                ranking = ranking.sort_values(by='Vendas_Fechadas_Valor', ascending=False)
                
                st.dataframe(ranking, use_container_width=True)
                
                # Totais do Período
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Faturado (Período)", f"R$ {ranking['Vendas_Fechadas_Valor'].sum():,.2f}")
                c2.metric("Total Perdido (Período)", f"R$ {ranking['Vendas_Perdidas_Valor'].sum():,.2f}")
                c3.metric("Total de Orçamentos", f"{ranking['Orcamentos_Qtd'].sum()}")
            else:
                st.info("Sem dados para o período selecionado.")

        with tab2:
            st.subheader("Detalhe das Interações")
            colunas_view = ['Data', 'Nome_Cliente', 'Tipo', 'Resumo', 'Valor_Proposta', 'Vendedor']
            # Garante que as colunas existem antes de mostrar
            cols_finais = [c for c in colunas_view if c in df_filtrado.columns]
            st.dataframe(df_filtrado[cols_finais].sort_values(by='Data', ascending=False), use_container_width=True)
        
    # --- ÁREA DO VENDEDOR ---
    else:
        st.title(f"Área: {usuario_logado}")
        
        if meus_clientes.empty:
            st.error("Nenhum cliente vinculado.")
        else:
            col_esq, col_dir = st.columns([1, 1])
            with col_esq:
                st.subheader("Sua Carteira")
                opcoes_status = ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO', '👎 VENDA PERDIDA', '⭐ VENDA RECENTE', '🟢 ATIVO']
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

                        # CORREÇÃO: Exibição da Data da Última Compra
                        c1, c2 = st.columns(2)
                        c1.write(f"📞 {dados['Telefone_Contato1']}")
                        
                        if pd.notna(dados['Data_Ultima_Compra']):
                            c2.write(f"📅 Última Compra: **{dados['Data_Ultima_Compra'].strftime('%d/%m/%Y')}**")
                        else:
                            c2.write("📅 Última Compra: **Nunca / Novo**")
                        
                        st.divider()
                        
                        # REMOVIDO ST.FORM PARA PERMITIR VALOR APARECER NA HORA
                        st.write("📝 **Registrar Atividade**")
                        
                        tipo = st.selectbox("Nova Ação", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada", "Venda Perdida", "Agendou Visita"], key="sel_acao")
                        
                        # CAMPO CONDICIONAL (Aparece instantaneamente agora)
                        valor_acao = 0.0
                        if tipo == "Orçamento Enviado":
                            st.markdown("**💲 Valor da Proposta?**")
                            valor_acao = st.number_input("R$:", min_value=0.0, step=100.0, key="vlr_acao")
                        
                        if tipo == "Venda Perdida" or tipo == "Venda Fechada":
                            st.markdown(f"**💸 Valor da Venda ({tipo})?**")
                            valor_acao = st.number_input("R$:", min_value=0.0, step=100.0, key="vlr_fechado")

                        obs = st.text_area("Obs:", key="obs_acao")
                        
                        # Botão normal (fora de form)
                        if st.button("✅ Salvar Histórico", type="primary"):
                            if salvar_interacao_nuvem(cliente_id, datetime.now(), tipo, obs, usuario_logado, valor_acao):
                                st.success("Salvo!")
                                st.rerun()
else:
    st.warning("Carregando base de dados...")
