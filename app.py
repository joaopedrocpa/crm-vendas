import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 9.1", layout="wide")

# --- CSS CORRIGIDO (VISUAL DARK & CARDS LEGÍVEIS) ---
st.markdown("""
<style>
    /* Ajuste da Barra Lateral */
    [data-testid="stSidebar"] {min-width: 300px;}
    
    /* CORREÇÃO DOS CARDS (KPIs) - Fundo Escuro e Letra Clara */
    div[data-testid="stMetric"] {
        background-color: #1E1E1E; /* Fundo Preto Suave */
        border: 1px solid #333;
        padding: 15px;
        border-radius: 8px;
    }
    div[data-testid="stMetricLabel"] {
        color: #b0b3b8 !important; /* Cor do Título (Cinza Claro) */
        font-weight: bold;
    }
    div[data-testid="stMetricValue"] {
        color: #ffffff !important; /* Cor do Valor (Branco Puro) */
    }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES DE FORMATAÇÃO E LIMPEZA ---
def formatar_moeda_visual(valor):
    if pd.isna(valor) or str(valor).strip() == '': return "R$ 0,00"
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except: return str(valor)

def limpar_valor_monetario(valor):
    """
    Função Híbrida: Lê números puros (da nova versão) e textos antigos.
    """
    if pd.isna(valor): return 0.0
    
    # SE JÁ FOR NÚMERO (Solução Definitiva), retorna direto
    if isinstance(valor, (int, float)):
        return float(valor)

    s = str(valor).strip()
    if s == '': return 0.0
    
    # Limpeza de texto antigo (R$ 1.000,00)
    s = s.replace('R$', '').strip()
    
    # Lógica Brasileira para Textos Antigos:
    # Remove ponto de milhar e troca vírgula decimal por ponto
    s = s.replace('.', '').replace(',', '.')
    
    try:
        return float(s)
    except:
        return 0.0

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

def formatar_data_br(data):
    if pd.isna(data) or str(data).strip() == '': return "-"
    try:
        return pd.to_datetime(data).strftime('%d/%m/%Y')
    except: return str(data)

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
        st.error(f"⚠️ Erro de Conexão: {e}")
        return None

# --- CARREGAMENTO DE DADOS ---
@st.cache_data(ttl=60)
def carregar_dados_completos():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None, None
    try:
        # 1. Config
        try:
            sheet_config = spreadsheet.worksheet("Config_Equipe")
            df_config = pd.DataFrame(sheet_config.get_all_records())
            for col in df_config.columns:
                df_config[col] = df_config[col].astype(str)
        except:
            st.error("Aba 'Config_Equipe' não encontrada!")
            return None, None, None

        # 2. Clientes
        try:
            sheet_clientes = spreadsheet.worksheet("Clientes")
            df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
        except Exception as e:
            st.error(f"Erro aba Clientes: {e}")
            return None, None, None
        
        # 3. Leads
        try:
            sheet_leads = spreadsheet.worksheet("Novos_Leads")
            dados_leads = sheet_leads.get_all_records()
            df_leads = pd.DataFrame(dados_leads)
        except: df_leads = pd.DataFrame() 
            
        # 4. Join
        if not df_leads.empty:
            df_leads = df_leads.astype(str)
            df_protheus = df_protheus.astype(str)
            df_clientes = pd.concat([df_protheus, df_leads], ignore_index=True)
        else:
            df_clientes = df_protheus

        # Tratamento Clientes
        if not df_clientes.empty:
            df_clientes.columns = df_clientes.columns.str.strip()
            df_clientes['ID_Cliente_CNPJ_CPF'] = df_clientes['ID_Cliente_CNPJ_CPF'].astype(str)
            if 'Ultimo_Vendedor' in df_clientes.columns:
                df_clientes['Ultimo_Vendedor'] = df_clientes['Ultimo_Vendedor'].astype(str).str.strip()
            
            if 'Total_Compras' in df_clientes.columns:
                df_clientes['Total_Compras'] = df_clientes['Total_Compras'].apply(limpar_valor_monetario)
            
            if 'Data_Ultima_Compra' in df_clientes.columns:
                df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')
        
        # 5. Interações
        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
            if not df_interacoes.empty:
                if 'Valor_Proposta' in df_interacoes.columns:
                    df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].apply(limpar_valor_monetario)
                
                if 'Data' in df_interacoes.columns:
                    df_interacoes['Data_Obj'] = pd.to_datetime(df_interacoes['Data'], dayfirst=True, errors='coerce').dt.date
                
                if 'CNPJ_Cliente' in df_interacoes.columns:
                    mapa_nomes = dict(zip(df_clientes['ID_Cliente_CNPJ_CPF'].astype(str), df_clientes['Nome_Fantasia']))
                    df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
                    df_interacoes['Nome_Cliente'] = df_interacoes['CNPJ_Cliente'].map(mapa_nomes).fillna("Cliente Carteira")
        except:
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor', 'Valor_Proposta'])

        return df_clientes, df_interacoes, df_config
    except Exception as e:
        st.error(f"Erro Crítico de Leitura: {e}")
        return None, None, None

# --- SALVAMENTO (AGORA SALVA NÚMERO PURO) ---
def salvar_interacao_nuvem(cnpj, data_obj, tipo, resumo, vendedor, valor=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        data_str = data_obj.strftime('%d/%m/%Y')
        
        # MUDANÇA CRUCIAL: Envia o FLOAT puro, não string
        # O Google Sheets vai receber 150.25 e tratar como número
        valor_save = float(valor)
        
        sheet.append_row([str(cnpj), data_str, tipo, resumo, vendedor, valor_save])
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
        
        # MUDANÇA CRUCIAL: Envia o FLOAT puro
        valor_save = float(valor_inicial)
        
        sheet_interacoes.append_row([str(cnpj), data_str, primeira_acao, resumo_inicial, vendedor, valor_save])
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
    if not nome or not doc or origem == "SELECIONE..." or acao == "SELECIONE...":
        st.error("Campos obrigatórios!")
    else:
        if salvar_novo_lead_completo(doc, nome, contato, tel, usuario_logado, origem, acao, resumo, val):
            st.success("Salvo!")
            for k in ["novo_nome", "novo_doc", "novo_contato", "novo_tel", "novo_resumo"]:
                st.session_state[k] = ""
            st.session_state["novo_val"] = 0.0
            st.session_state["novo_origem"] = "SELECIONE..."
            st.session_state["novo_acao"] = "SELECIONE..."

def processar_salvamento_vendedor(cid, usuario_logado, tipo_selecionado):
    obs = st.session_state["obs_temp"]
    val = st.session_state["val_temp"]
    if salvar_interacao_nuvem(cid, datetime.now(), tipo_selecionado, obs, usuario_logado, val):
        st.success("Salvo!")
        st.session_state["obs_temp"] = ""
        st.session_state["val_temp"] = 0.0

# --- APP ---
try:
    st.sidebar.title("🚀 CRM Master 9.1")
    
    with st.spinner("Conectando ao banco de dados..."):
        df, df_interacoes, df_config = carregar_dados_completos()

    if df is not None and not df_config.empty:
        # --- LOGIN BLINDADO ---
        usuarios_validos = sorted(df_config['Usuario'].unique().tolist())
        
        if 'logado' not in st.session_state: st.session_state['logado'] = False
        if 'usuario_atual' not in st.session_state: st.session_state['usuario_atual'] = ""
        
        if not st.session_state['logado']:
            st.sidebar.markdown("### 🔒 Acesso Restrito")
            usuario_input = st.sidebar.selectbox("Usuário:", usuarios_validos)
            senha_input = st.sidebar.text_input("Senha:", type="password")
            
            if st.sidebar.button("Entrar"):
                user_data = df_config[df_config['Usuario'] == usuario_input].iloc[0]
                if str(user_data['Senha']).strip() == str(senha_input).strip():
                    st.session_state['logado'] = True
                    st.session_state['usuario_atual'] = usuario_input
                    st.rerun()
                else:
                    st.sidebar.error("Senha incorreta!")
            st.stop()
        
        # --- LOGADO ---
        usuario_logado = st.session_state['usuario_atual']
        if st.sidebar.button(f"Sair ({usuario_logado})"):
            st.session_state['logado'] = False
            st.rerun()

        user_data = df_config[df_config['Usuario'] == usuario_logado].iloc[0]
        tipo_usuario = str(user_data['Tipo']).upper().strip()
        carteiras_permitidas = [x.strip() for x in str(user_data['Carteira_Alvo']).split(',')]

        # --- LÓGICA STATUS ---
        hoje = datetime.now().date()
        if 'Data_Ultima_Compra' in df.columns:
            df['Dias_Sem_Comprar'] = (pd.Timestamp(hoje) - df['Data_Ultima_Compra']).dt.days
        else: df['Dias_Sem_Comprar'] = 0

        def calcular_status(linha):
            cnpj = linha['ID_Cliente_CNPJ_CPF']
            if not df_interacoes.empty and 'CNPJ_Cliente' in df_interacoes.columns:
                cnpj_str = str(cnpj)
                filtro = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj_str]
                if not filtro.empty and 'Data_Obj' in filtro.columns:
                    filtro = filtro.sort_values(by='Data_Obj')
                    ultima = filtro.iloc[-1]
                    try:
                        if pd.notna(ultima['Data_Obj']):
                            dias_acao = (hoje - ultima['Data_Obj']).days
                            if ultima['Tipo'] == 'Orçamento Enviado': return '⚠️ FOLLOW-UP' if dias_acao >= 5 else '⏳ NEGOCIAÇÃO'
                            if ultima['Tipo'] == 'Venda Fechada': return '⭐ VENDA RECENTE'
                            if ultima['Tipo'] == 'Venda Perdida': return '👎 VENDA PERDIDA'
                            if ultima['Tipo'] == 'Ligação Realizada': return '📞 CONTATADO RECENTEMENTE'
                            if ultima['Tipo'] == 'WhatsApp Enviado': return '💬 WHATSAPP INICIADO'
                    except: pass
            if pd.isna(linha['Dias_Sem_Comprar']): return '🆕 NOVO S/ INTERAÇÃO'
            if linha['Dias_Sem_Comprar'] >= 60: return '🔴 RECUPERAR'
            return '🟢 ATIVO'
        
        df['Status'] = df.apply(calcular_status, axis=1)

        # --- MENU CADASTRO ---
        if tipo_usuario == "VENDEDOR" or "TODOS" in carteiras_permitidas:
            st.sidebar.markdown("---")
            with st.sidebar.expander("➕ Cadastrar Novo Lead"):
                for k in ["novo_nome", "novo_doc", "novo_contato", "novo_tel", "novo_resumo"]:
                    if k not in st.session_state: st.session_state[k] = ""
                if "novo_val" not in st.session_state: st.session_state["novo_val"] = 0.0
                if "novo_origem" not in st.session_state: st.session_state["novo_origem"] = "SELECIONE..."
                if "novo_acao" not in st.session_state: st.session_state["novo_acao"] = "SELECIONE..."

                st.text_input("Nome:", key="novo_nome")
                st.text_input("CPF/CNPJ:", key="novo_doc")
                st.text_input("Contato:", key="novo_contato")
                st.text_input("Telefone:", key="novo_tel")
                c1, c2 = st.columns(2)
                c1.selectbox("Origem:", ["SELECIONE...", "SZ.CHAT", "LIGAÇÃO", "PRESENCIAL", "E-MAIL", "INDICAÇÃO"], key="novo_origem")
                c2.selectbox("Ação:", ["SELECIONE...", "Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"], key="novo_acao")
                if st.session_state["novo_acao"] == "Orçamento Enviado":
                    st.number_input("Valor (R$):", step=0.01, format="%.2f", key="novo_val")
                st.text_area("Resumo:", key="novo_resumo")
                st.button("💾 SALVAR LEAD", type="primary", on_click=processar_salvamento_lead, args=(usuario_logado,))

        # --- ROTEAMENTO ---
        if "TODOS" in carteiras_permitidas:
            meus_clientes = df
            minhas_interacoes = df_interacoes
        else:
            if 'Ultimo_Vendedor' in df.columns:
                meus_clientes = df[df['Ultimo_Vendedor'].isin(carteiras_permitidas)]
            else: meus_clientes = pd.DataFrame()
            if not df_interacoes.empty and 'Vendedor' in df_interacoes.columns:
                minhas_interacoes = df_interacoes[df_interacoes['Vendedor'].isin(carteiras_permitidas)]
            else: minhas_interacoes = pd.DataFrame()

        # --- VIEW GESTOR ---
        if tipo_usuario == "GESTOR":
            st.title(f"📊 Gestão: {usuario_logado}")
            
            with st.container(border=True):
                col_f1, col_f2, col_f3 = st.columns(3)
                d_ini = col_f1.date_input("De:", value=hoje - timedelta(days=30), format="DD/MM/YYYY")
                d_fim = col_f2.date_input("Até:", value=hoje, format="DD/MM/YYYY")
                opcoes_tipo = minhas_interacoes['Tipo'].unique().tolist() if not minhas_interacoes.empty else []
                tipos_sel = col_f3.multiselect("Filtrar Tipos:", options=opcoes_tipo, default=opcoes_tipo)

            if not minhas_interacoes.empty and 'Data_Obj' in minhas_interacoes.columns:
                mask_data = (minhas_interacoes['Data_Obj'] >= d_ini) & (minhas_interacoes['Data_Obj'] <= d_fim)
                df_filtered = minhas_interacoes[mask_data].copy()
                
                vlr_orcado = df_filtered[df_filtered['Tipo'] == 'Orçamento Enviado']['Valor_Proposta'].sum()
                vlr_perdido = df_filtered[df_filtered['Tipo'] == 'Venda Perdida']['Valor_Proposta'].sum()
                vlr_fechado = df_filtered[df_filtered['Tipo'] == 'Venda Fechada']['Valor_Proposta'].sum()
                qtd_fechado = len(df_filtered[df_filtered['Tipo'] == 'Venda Fechada'])
                qtd_total = len(df_filtered)
                df_tabela = df_filtered[df_filtered['Tipo'].isin(tipos_sel)]
            else:
                vlr_orcado = vlr_perdido = vlr_fechado = 0.0
                qtd_fechado = qtd_total = 0
                df_filtered = pd.DataFrame()
                df_tabela = pd.DataFrame()

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("💰 Orçado", formatar_moeda_visual(vlr_orcado))
            k2.metric("👎 Perdido", formatar_moeda_visual(vlr_perdido))
            k3.metric("✅ Fechado", formatar_moeda_visual(vlr_fechado), f"{qtd_fechado} vendas")
            k4.metric("📞 Interações", f"{qtd_total}")
            
            st.divider()

            t1, t2 = st.tabs(["🏆 Ranking", "📝 Detalhes"])
            with t1:
                if not df_filtered.empty:
                    df_filtered['Is_Orcamento'] = (df_filtered['Tipo'] == 'Orçamento Enviado').astype(int)
                    df_filtered['Is_Fechado'] = (df_filtered['Tipo'] == 'Venda Fechada').astype(int)
                    df_filtered['Valor_Aux_Ranking'] = df_filtered.apply(lambda x: x['Valor_Proposta'] if x['Tipo'] == 'Venda Fechada' else 0.0, axis=1)

                    ranking = df_filtered.groupby('Vendedor').agg(
                        Orcamentos=('Is_Orcamento', 'sum'),
                        Fechados=('Is_Fechado', 'sum'),
                        Total_Vendido=('Valor_Aux_Ranking', 'sum')
                    ).reset_index().sort_values('Total_Vendido', ascending=False)
                    ranking['Total_Vendido'] = ranking['Total_Vendido'].apply(formatar_moeda_visual)
                    st.dataframe(ranking, use_container_width=True)
                else: st.info("Sem dados.")
            with t2:
                if not df_tabela.empty:
                    view = df_tabela[['Data_Obj', 'Nome_Cliente', 'Tipo', 'Resumo', 'Valor_Proposta', 'Vendedor']].copy()
                    view['Valor_Proposta'] = view['Valor_Proposta'].apply(formatar_moeda_visual)
                    view.rename(columns={'Data_Obj': 'Data'}, inplace=True)
                    st.dataframe(view, use_container_width=True, hide_index=True, column_config={"Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY")})
                else: st.info("Nenhuma interação.")

        # --- VIEW VENDEDOR ---
        else:
            st.title(f"💼 Vendas: {usuario_logado}")
            if meus_clientes.empty:
                st.warning("Nenhum cliente atribuído a você.")
            else:
                c_esq, c_dir = st.columns([1, 1])
                with c_esq:
                    st.subheader("Carteira")
                    termo_busca = st.text_input("🔍 Buscar (CNPJ ou Nome):", placeholder="Digite...")
                    if termo_busca:
                        termo_busca = termo_busca.upper()
                        lista = meus_clientes[
                            meus_clientes['Nome_Fantasia'].str.upper().str.contains(termo_busca, na=False) |
                            meus_clientes['ID_Cliente_CNPJ_CPF'].astype(str).str.contains(termo_busca, na=False)
                        ]
                        if lista.empty: st.warning("Não encontrado.")
                    else:
                        ops = ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO', '👎 VENDA PERDIDA', '⭐ VENDA RECENTE', '🟢 ATIVO']
                        sel_status = st.multiselect("Filtrar:", ops, default=['⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO'])
                        lista = meus_clientes[meus_clientes['Status'].isin(sel_status)].sort_values('Status', ascending=False)
                        if lista.empty: st.info("Nenhum cliente com estes filtros.")

                    cid = None
                    if not lista.empty:
                        with st.container(height=600):
                            cid = st.radio("Selecione:", lista['ID_Cliente_CNPJ_CPF'].tolist(), format_func=lambda x: f"{formatar_documento(x)} | {lista[lista['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]}")

                with c_dir:
                    if cid and not meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cid].empty:
                        cli = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cid].iloc[0]
                        with st.container(border=True):
                            st.markdown(f"### {cli['Nome_Fantasia']}")
                            doc_fmt = formatar_documento(cli['ID_Cliente_CNPJ_CPF'])
                            st.caption(f"🆔 {doc_fmt}")
                            st.info(f"Status: **{cli['Status']}**")
                            
                            st.divider()
                            st.markdown("#### 📋 Dados do Cliente")
                            col_d1, col_d2 = st.columns(2)
                            
                            v_contato = cli.get('Nome_Contato', '-') if 'Nome_Contato' in cli else cli.get('Contato', '-')
                            v_email = cli.get('Email', '-') if 'Email' in cli else '-'
                            v_tel1 = cli.get('Telefone_Contato1', '-') if 'Telefone_Contato1' in cli else '-'
                            col_d1.write(f"**👤 Contato:** {v_contato}")
                            col_d1.write(f"**📧 Email:** {v_email}")
                            col_d1.write(f"**📞 Tel 1:** {v_tel1}")

                            v_tel2 = cli.get('Telefone_Contato2', '-') if 'Telefone_Contato2' in cli else '-'
                            v_compra = formatar_moeda_visual(cli['Total_Compras']) if 'Total_Compras' in cli else "R$ 0,00"
                            v_dt_compra = formatar_data_br(cli['Data_Ultima_Compra']) if 'Data_Ultima_Compra' in cli else "-"
                            v_dono = cli.get('Ultimo_Vendedor', '-')

                            col_d2.write(f"**📱 Tel 2:** {v_tel2}")
                            col_d2.write(f"**💰 Total Compras:** {v_compra}")
                            col_d2.write(f"**📅 Última Compra:** {v_dt_compra}")
                            col_d2.write(f"**👔 Carteira:** {v_dono}")
                            
                            if 'Cidade' in cli and 'UF' in cli:
                                st.write(f"📍 **Local:** {cli['Cidade']}/{cli['UF']}")

                            st.divider()
                            if not df_interacoes.empty and 'CNPJ_Cliente' in df_interacoes.columns:
                                hist = df_interacoes[df_interacoes['CNPJ_Cliente'] == str(cid)].sort_values('Data_Obj', ascending=False)
                                if not hist.empty:
                                    last = hist.iloc[0]
                                    dt_fmt = pd.to_datetime(last['Data']).strftime('%d/%m/%Y')
                                    st.warning(f"🕒 **Última Ação ({dt_fmt}):** {last['Tipo']}\n\n_{last['Resumo']}_")
                                    if last['Tipo'] == 'Orçamento Enviado' and last['Valor_Proposta'] > 0:
                                        v_fmt = formatar_moeda_visual(last['Valor_Proposta'])
                                        st.info(f"💰 **Proposta Aberta:** {v_fmt}")
                            
                            st.markdown("#### 📝 Nova Interação")
                            if "obs_temp" not in st.session_state: st.session_state["obs_temp"] = ""
                            if "val_temp" not in st.session_state: st.session_state["val_temp"] = 0.0
                            
                            tipo = st.selectbox("Ação:", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada", "Venda Perdida", "Agendou Visita"])
                            if tipo in ["Orçamento Enviado", "Venda Fechada", "Venda Perdida"]:
                                st.number_input("Valor (R$):", step=0.01, format="%.2f", key="val_temp")
                            st.text_area("Obs:", key="obs_temp")
                            st.button("✅ Salvar", type="primary", on_click=processar_salvamento_vendedor, args=(cid, usuario_logado, tipo))
                    else:
                        st.info("👈 Selecione um cliente na lista.")

except Exception as e:
    st.error(f"Erro Fatal no Sistema: {e}")
