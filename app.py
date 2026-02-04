import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 6.0", layout="wide")

# --- FUNÇÕES DE LIMPEZA E FORMATAÇÃO ---
def formatar_moeda(valor):
    if pd.isna(valor) or valor == '': return "R$ 0,00"
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except: return str(valor)

def limpar_valor_monetario(valor):
    """Transforma qualquer formato (R$ 1.000,00 ou 1000.00) em float puro"""
    if pd.isna(valor): return 0.0
    s = str(valor).strip()
    s = s.replace('R$', '').strip()
    # Se tiver virgula e ponto, assume padrão BR (1.000,00) -> remove ponto, troca virgula por ponto
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except:
        return 0.0

def converter_data_br(data_str):
    """Converte string ou datetime para data pura (sem hora)"""
    try:
        return pd.to_datetime(data_str, dayfirst=True).date()
    except:
        return None

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

        # Tratamento Clientes
        if not df_clientes.empty:
            df_clientes['ID_Cliente_CNPJ_CPF'] = df_clientes['ID_Cliente_CNPJ_CPF'].astype(str)
            df_clientes['Total_Compras'] = df_clientes['Total_Compras'].apply(limpar_valor_monetario)
            df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')

        # 4. Interações
        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
            
            if not df_interacoes.empty:
                # Limpeza Robusta
                df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].apply(limpar_valor_monetario)
                
                # Converte para data pura para facilitar filtros
                df_interacoes['Data_Obj'] = pd.to_datetime(df_interacoes['Data'], dayfirst=True, errors='coerce').dt.date
                
                # Mapeia Nomes
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

def salvar_interacao_nuvem(cnpj, data_obj, tipo, resumo, vendedor, valor=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        
        # CORREÇÃO: Salva apenas a DATA (DD/MM/YYYY) sem hora
        data_str = data_obj.strftime('%d/%m/%Y')
        valor_str = f"{valor:.2f}".replace('.', ',')
        
        sheet.append_row([str(cnpj), data_str, tipo, resumo, vendedor, valor_str])
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
        data_str = datetime.now().strftime('%d/%m/%Y')
        valor_str = f"{valor_inicial:.2f}".replace('.', ',')
        
        sheet_interacoes.append_row([str(cnpj), data_str, primeira_acao, resumo_inicial, vendedor, valor_str])
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao criar lead: {e}")
        return False

# --- INTERFACE ---
st.sidebar.title("🚀 CRM Master 6.0")

df, df_interacoes, df_config = carregar_dados_completos()

if df is not None and not df.empty:
    hoje = datetime.now().date() # Trabalhar apenas com data pura
    df['Dias_Sem_Comprar'] = (pd.Timestamp(hoje) - df['Data_Ultima_Compra']).dt.days

    # --- LÓGICA DE STATUS ---
    def calcular_status(linha):
        cnpj = linha['ID_Cliente_CNPJ_CPF']
        if not df_interacoes.empty:
            cnpj_str = str(cnpj)
            # Filtra interações desse cliente
            filtro = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj_str]
            if not filtro.empty:
                # Pega a última baseada na data
                # Garante conversão para data para comparação
                ultima = filtro.sort_values(by='Data_Obj').iloc[-1]
                
                try:
                    if pd.notna(ultima['Data_Obj']):
                        dias_acao = (hoje - ultima['Data_Obj']).days
                        
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

    # --- CADASTRO LEAD ---
    if usuario_logado != "GESTOR":
        st.sidebar.markdown("---")
        with st.sidebar.expander("➕ Cadastrar Novo Lead"):
            st.write("**Dados do Cliente**")
            # Usa session state para limpar depois
            nome = st.text_input("Nome:", key="novo_nome")
            doc = st.text_input("CPF/CNPJ:", key="novo_doc")
            contato = st.text_input("Contato:", key="novo_contato")
            tel = st.text_input("Telefone:", key="novo_tel")
            
            c1, c2 = st.columns(2)
            origem = c1.selectbox("Origem:", ["SELECIONE...", "SZ.CHAT", "LIGAÇÃO", "PRESENCIAL", "E-MAIL", "INDICAÇÃO"], key="novo_origem")
            acao = c2.selectbox("Ação:", ["SELECIONE...", "Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"], key="novo_acao")
            
            val = 0.0
            if acao == "Orçamento Enviado":
                val = st.number_input("Valor (R$):", step=100.0, key="novo_val")
            
            resumo = st.text_area("Resumo:", key="novo_resumo")
            
            if st.button("💾 SALVAR LEAD", type="primary"):
                if not nome or not doc or origem == "SELECIONE..." or acao == "SELECIONE...":
                    st.error("Campos obrigatórios vazios!")
                else:
                    if salvar_novo_lead_completo(doc, nome, contato, tel, usuario_logado, origem, acao, resumo, val):
                        st.success("Lead Salvo!")
                        # Limpa chaves manualmente para dar reset visual
                        for k in ["novo_nome", "novo_doc", "novo_contato", "novo_tel", "novo_resumo"]:
                            st.session_state[k] = ""
                        st.session_state["novo_val"] = 0.0
                        st.rerun()

    # --- PERMISSÕES ---
    if usuario_logado == "GESTOR":
        meus_clientes = df
    else:
        if not df_config.empty:
            regra = df_config[df_config['Usuario_Login'] == usuario_logado]
            if not regra.empty:
                carteiras = regra.iloc[0]['Carteiras_Visiveis']
                if "TODOS" in carteiras.upper(): meus_clientes = df 
                else:
                    lista = [n.strip() for n in carteiras.split(',')]
                    meus_clientes = df[df['Ultimo_Vendedor'].isin(lista)]
            else:
                meus_clientes = pd.DataFrame()
        else:
            meus_clientes = df[df['Ultimo_Vendedor'] == usuario_logado]

    # --- GESTOR ---
    if usuario_logado == "GESTOR":
        st.title("📊 Painel Geral & Financeiro")

        # 1. FILTROS (Com formato BR e lógica corrigida)
        with st.container(border=True):
            col_f1, col_f2, col_f3 = st.columns(3)
            
            # Data padrão: 30 dias atrás até hoje
            ini_padrao = hoje - timedelta(days=30)
            
            # CORREÇÃO: format="DD/MM/YYYY" força visual brasileiro no calendário
            d_ini = col_f1.date_input("De:", value=ini_padrao, format="DD/MM/YYYY")
            d_fim = col_f2.date_input("Até:", value=hoje, format="DD/MM/YYYY")
            
            opcoes_tipo = df_interacoes['Tipo'].unique().tolist() if not df_interacoes.empty else []
            tipos_sel = col_f3.multiselect("Filtrar Tipos:", options=opcoes_tipo, default=opcoes_tipo)

        # 2. CÁLCULOS
        if not df_interacoes.empty:
            # Filtro de Data (Comparando date com date)
            mask_data = (df_interacoes['Data_Obj'] >= d_ini) & (df_interacoes['Data_Obj'] <= d_fim)
            df_filtered = df_interacoes[mask_data]
            
            # Filtro de Tipo (Apenas para tabelas, KPIs usam a base de data)
            df_tabela = df_filtered[df_filtered['Tipo'].isin(tipos_sel)]
            
            # KPIs
            vlr_orcado = df_filtered[df_filtered['Tipo'] == 'Orçamento Enviado']['Valor_Proposta'].sum()
            vlr_perdido = df_filtered[df_filtered['Tipo'] == 'Venda Perdida']['Valor_Proposta'].sum()
            vlr_fechado = df_filtered[df_filtered['Tipo'] == 'Venda Fechada']['Valor_Proposta'].sum()
            qtd_fechado = len(df_filtered[df_filtered['Tipo'] == 'Venda Fechada'])
            qtd_total = len(df_filtered)
        else:
            vlr_orcado = vlr_perdido = vlr_fechado = 0.0
            qtd_fechado = qtd_total = 0
            df_tabela = pd.DataFrame()

        # 3. KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("💰 Orçado", formatar_moeda(vlr_orcado))
        k2.metric("👎 Perdido", formatar_moeda(vlr_perdido))
        k3.metric("✅ Fechado", formatar_moeda(vlr_fechado), f"{qtd_fechado} vendas")
        k4.metric("📞 Interações", f"{qtd_total}")
        
        st.divider()

        # 4. RANKING & DADOS
        t1, t2 = st.tabs(["🏆 Ranking", "📝 Detalhes"])
        with t1:
            if not df_filtered.empty:
                ranking = df_filtered.groupby('Vendedor').agg(
                    Orcamentos=('Tipo', lambda x: (x == 'Orçamento Enviado').sum()),
                    Fechados=('Tipo', lambda x: (x == 'Venda Fechada').sum()),
                    R$_Fechado=('Valor_Proposta', lambda x: x[df_filtered['Tipo'] == 'Venda Fechada'].sum())
                ).reset_index().sort_values('R$_Fechado', ascending=False)
                
                # Formata visualmente
                ranking['R$_Fechado'] = ranking['R$_Fechado'].apply(formatar_moeda)
                st.dataframe(ranking, use_container_width=True)
            else:
                st.info("Sem dados no período.")
        
        with t2:
            if not df_tabela.empty:
                # Prepara tabela bonita
                view = df_tabela[['Data', 'Nome_Cliente', 'Tipo', 'Resumo', 'Valor_Proposta', 'Vendedor']].copy()
                view['Data'] = view['Data'].apply(lambda x: pd.to_datetime(x).strftime('%d/%m/%Y') if pd.notna(x) else "-")
                view['Valor_Proposta'] = view['Valor_Proposta'].apply(formatar_moeda)
                st.dataframe(view, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhuma interação encontrada com estes filtros.")

    # --- VENDEDOR ---
    else:
        st.title(f"Área: {usuario_logado}")
        
        if meus_clientes.empty:
            st.error("Carteira vazia.")
        else:
            c_esq, c_dir = st.columns([1, 1])
            with c_esq:
                st.subheader("Carteira")
                # Filtros
                ops = ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO', '👎 VENDA PERDIDA', '⭐ VENDA RECENTE', '🟢 ATIVO']
                sel_status = st.multiselect("Status:", ops, default=['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO'])
                
                lista = meus_clientes[meus_clientes['Status'].isin(sel_status)].sort_values('Status', ascending=False)
                
                if lista.empty: st.info("Nada aqui.")
                else:
                    cid = st.radio("Cliente:", lista['ID_Cliente_CNPJ_CPF'].tolist(), 
                                  format_func=lambda x: f"{lista[lista['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]}")

            with c_dir:
                if 'cid' in locals() and cid:
                    # Pega dados do cliente
                    cli = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cid].iloc[0]
                    
                    with st.container(border=True):
                        st.markdown(f"### {cli['Nome_Fantasia']}")
                        st.caption(f"CNPJ: {cli['ID_Cliente_CNPJ_CPF']}")
                        st.info(f"Status: **{cli['Status']}**")
                        
                        # --- NOVO: HISTÓRICO RECENTE NA TELA ---
                        if not df_interacoes.empty:
                            hist = df_interacoes[df_interacoes['CNPJ_Cliente'] == str(cid)].sort_values('Data_Obj', ascending=False)
                            if not hist.empty:
                                last = hist.iloc[0]
                                dt_fmt = pd.to_datetime(last['Data']).strftime('%d/%m/%Y')
                                st.warning(f"🕒 **Última Ação ({dt_fmt}):** {last['Tipo']}\n\n_{last['Resumo']}_")

                        st.divider()
                        
                        # FORMULÁRIO COM LIMPEZA AUTOMÁTICA
                        # Usamos chaves dinâmicas ou session state
                        
                        # Inicializa session state se não existir
                        if "obs_temp" not in st.session_state: st.session_state["obs_temp"] = ""
                        if "val_temp" not in st.session_state: st.session_state["val_temp"] = 0.0
                        
                        tipo = st.selectbox("Ação:", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada", "Venda Perdida", "Agendou Visita"])
                        
                        val = 0.0
                        if tipo in ["Orçamento Enviado", "Venda Fechada", "Venda Perdida"]:
                            val = st.number_input("Valor (R$):", step=100.0, key="val_temp")
                        
                        obs = st.text_area("Obs:", key="obs_temp")
                        
                        if st.button("✅ Salvar", type="primary"):
                            # Salva Data de Hoje Pura
                            if salvar_interacao_nuvem(cid, datetime.now(), tipo, obs, usuario_logado, val):
                                st.success("Salvo!")
                                # RESET MANUAL DOS CAMPOS
                                st.session_state["obs_temp"] = ""
                                st.session_state["val_temp"] = 0.0
                                st.rerun()

else:
    st.warning("Carregando...")
