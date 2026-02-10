import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import random
import string
import re
import time

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="CRM Master 22.1 Turbo", layout="wide")
URL_LOGO = "https://cdn-icons-png.flaticon.com/512/9187/9187604.png"

# --- CSS (VISUAL DARK PREMIUM) ---
st.markdown("""
<style>
    [data-testid="stSidebar"] {min-width: 300px;}
    div[data-testid="stMetric"] {
        background-color: #262730;
        border: 1px solid #464b5c;
        padding: 15px;
        border-radius: 8px;
    }
    div[data-testid="stMetricLabel"] {color: #b0b3b8 !important; font-weight: bold;}
    div[data-testid="stMetricValue"] {color: #ffffff !important;}
    .stButton button {width: 100%; font-weight: bold;}
    .stProgress > div > div > div > div {background-color: #00ff00;}
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES DE AJUDA (Helpers) ---

def gerar_id_proposta():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))

def extrair_id(texto):
    if pd.isna(texto): return None
    match = re.search(r'(#[A-Z0-9]{4})', str(texto))
    return match.group(1) if match else None

def extrair_pedido_protheus(texto):
    if pd.isna(texto): return None
    match = re.search(r'\[PROTHEUS\] Pedido: (\w+)', str(texto))
    return match.group(1) if match else None

def limpar_valor_inteiro(valor):
    if pd.isna(valor) or str(valor).strip() == '': return 0
    if isinstance(valor, (int, float)): return int(valor)
    s = str(valor).upper().replace('R$', '').strip()
    if ',' in s: s = s.split(',')[0]
    s = s.replace('.', '')
    s = re.sub(r'[^\d]', '', s)
    if not s: return 0
    return int(s)

def formatar_moeda_visual(valor):
    if pd.isna(valor): return "R$ 0"
    try:
        val = int(valor)
        return f"R$ {val:,.0f}".replace(',', '.')
    except: return "R$ 0"

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
    try: return pd.to_datetime(data).strftime('%d/%m/%Y')
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

# --- CARREGAMENTO OTIMIZADO (Separado por Frequência) ---

# 1. Config e Clientes (Pesado, cache longo de 1 hora)
@st.cache_data(ttl=3600) 
def carregar_dados_estaticos():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None
    
    # Config
    try:
        sheet_config = spreadsheet.worksheet("Config_Equipe")
        df_config = pd.DataFrame(sheet_config.get_all_records())
        for col in df_config.columns: df_config[col] = df_config[col].astype(str)
        # Metas
        cols_meta = ['Meta_Fat', 'Meta_Clientes', 'Meta_Atividades']
        for col in cols_meta:
            if col in df_config.columns: df_config[col] = df_config[col].apply(limpar_valor_inteiro)
            else: df_config[col] = 0
    except: df_config = pd.DataFrame()

    # Clientes (Protheus)
    try:
        sheet_clientes = spreadsheet.worksheet("Clientes")
        df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
    except: df_protheus = pd.DataFrame()

    # Tratamento Clientes
    if not df_protheus.empty:
        df_protheus.columns = df_protheus.columns.str.strip()
        df_protheus['ID_Cliente_CNPJ_CPF'] = df_protheus['ID_Cliente_CNPJ_CPF'].astype(str)
        if 'Total_Compras' in df_protheus.columns:
            df_protheus['Total_Compras'] = df_protheus['Total_Compras'].apply(limpar_valor_inteiro)
        if 'Data_Ultima_Compra' in df_protheus.columns:
            df_protheus['Data_Ultima_Compra'] = pd.to_datetime(df_protheus['Data_Ultima_Compra'], dayfirst=True, errors='coerce')
            
    return df_config, df_protheus

# 2. Interações e Leads (Dinâmico, cache médio)
@st.cache_data(ttl=600) 
def carregar_dados_dinamicos():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None
    
    # Leads
    try:
        sheet_leads = spreadsheet.worksheet("Novos_Leads")
        df_leads = pd.DataFrame(sheet_leads.get_all_records())
        if not df_leads.empty: df_leads = df_leads.astype(str)
    except: df_leads = pd.DataFrame()

    # Interações
    try:
        sheet_interacoes = spreadsheet.worksheet("Interacoes")
        df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
        if not df_interacoes.empty:
            if 'Valor_Proposta' in df_interacoes.columns:
                df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].apply(limpar_valor_inteiro)
            if 'Data' in df_interacoes.columns:
                df_interacoes['Data_Obj'] = pd.to_datetime(df_interacoes['Data'], dayfirst=True, errors='coerce').dt.date
            
            # CORREÇÃO CRÍTICA: Garantir que Nome_Cliente exista
            if 'Nome_Cliente' not in df_interacoes.columns:
                df_interacoes['Nome_Cliente'] = None
    except:
        df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor', 'Valor_Proposta', 'Nome_Cliente'])
        
    return df_leads, df_interacoes

# --- SALVAMENTO LOCAL (O SEGREDO DA VELOCIDADE) ---
def atualizar_estado_local(nova_linha_dict):
    """Insere o dado na memória RAM"""
    if 'dados_interacoes' in st.session_state:
        df_atual = st.session_state['dados_interacoes']
        novo_df = pd.DataFrame([nova_linha_dict])
        st.session_state['dados_interacoes'] = pd.concat([df_atual, novo_df], ignore_index=True)

def salvar_interacao_nuvem(cnpj, data_obj, tipo, resumo, vendedor, valor_inteiro):
    try:
        # 1. Salva no Google (Background)
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        data_str = data_obj.strftime('%d/%m/%Y')
        valor_save = int(valor_inteiro)
        
        id_prop = ""
        if tipo == "Orçamento Enviado":
            id_prop = f"#{gerar_id_proposta()}"
            resumo_final = f"{id_prop} {resumo}"
        else:
            resumo_final = resumo

        sheet.append_row([str(cnpj), data_str, tipo, resumo_final, vendedor, valor_save])
        
        # 2. Atualiza RAM Instantaneamente
        nova_linha = {
            'CNPJ_Cliente': str(cnpj),
            'Data': data_str,
            'Data_Obj': data_obj, 
            'Tipo': tipo,
            'Resumo': resumo_final,
            'Vendedor': vendedor,
            'Valor_Proposta': valor_save,
            'Nome_Cliente': "Atualizando..." 
        }
        atualizar_estado_local(nova_linha)
        return True
    except Exception as e:
        st.error(f"Erro Salvar: {e}")
        return False

def salvar_novo_lead_completo(cnpj, nome, contato, telefone, vendedor, origem, primeira_acao, resumo_inicial, valor_inteiro):
    try:
        spreadsheet = conectar_google_sheets()
        sheet_leads = spreadsheet.worksheet("Novos_Leads")
        nova_linha_lead = [str(cnpj), nome.upper(), contato, "NOVO LEAD", telefone, "", "", "0", "", "0", "", vendedor, origem]
        sheet_leads.append_row(nova_linha_lead)
        
        st.cache_data.clear() # Limpa cache para leads novos aparecerem
        
        sheet_interacoes = spreadsheet.worksheet("Interacoes")
        data_str = datetime.now().strftime('%d/%m/%Y')
        valor_save = int(valor_inteiro)
        id_prop = f"#{gerar_id_proposta()}" if primeira_acao == "Orçamento Enviado" else ""
        resumo_final = f"{id_prop} {resumo_inicial}" if id_prop else resumo_inicial
        
        sheet_interacoes.append_row([str(cnpj), data_str, primeira_acao, resumo_final, vendedor, valor_save])
        return True
    except Exception as e:
        st.error(f"Erro Lead: {e}")
        return False

# --- IMPORTAÇÃO PROTHEUS ---
def processar_arquivo_protheus(uploaded_file, df_existente):
    try:
        df_import = pd.read_excel(uploaded_file)
        cols_necessarias = ['DATA', 'CNPJ', 'VENDEDOR', 'VALOR', 'PEDIDO', 'STATUS']
        if not all(col in df_import.columns for col in cols_necessarias):
            return False, f"Arquivo inválido! Colunas necessárias: {cols_necessarias}"
        
        novas_linhas = []
        contador_novos = 0
        pulos = 0
        
        estado_crm = {}
        if not df_existente.empty:
            df_sorted = df_existente.sort_values('Data_Obj', ascending=True)
            for _, row in df_sorted.iterrows():
                ped_id = extrair_pedido_protheus(row['Resumo'])
                if ped_id: estado_crm[str(ped_id)] = {'valor': row['Valor_Proposta'], 'tipo': row['Tipo']}

        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        
        for index, row in df_import.iterrows():
            pedido_id = str(row['PEDIDO']).strip()
            valor_novo = limpar_valor_inteiro(row['VALOR'])
            status_orig = str(row['STATUS']).upper().strip()
            tipo_novo = "Orçamento Enviado"
            if "FECHADO" in status_orig or "FATURADO" in status_orig: tipo_novo = "Venda Fechada"
            elif "CANCELADO" in status_orig: tipo_novo = "Venda Perdida"
            
            acao_necessaria = False
            motivo_atualizacao = ""
            if pedido_id not in estado_crm:
                acao_necessaria = True
                contador_novos += 1
            else:
                dados_antigos = estado_crm[pedido_id]
                if abs(valor_novo - dados_antigos['valor']) > 1 and tipo_novo == "Orçamento Enviado" and dados_antigos['tipo'] == "Orçamento Enviado":
                    acao_necessaria = True; motivo_atualizacao = f"Atualização Valor"
                if tipo_novo != "Orçamento Enviado" and dados_antigos['tipo'] == "Orçamento Enviado":
                    acao_necessaria = True; motivo_atualizacao = "Baixa Protheus"
            
            if acao_necessaria:
                cnpj = ''.join(filter(str.isdigit, str(row['CNPJ'])))
                try: data_obj = pd.to_datetime(row['DATA']).strftime('%d/%m/%Y')
                except: data_obj = datetime.now().strftime('%d/%m/%Y')
                crm_id_tag = f"#{gerar_id_proposta()} " if tipo_novo == "Orçamento Enviado" and pedido_id not in estado_crm else ""
                resumo_txt = f"{crm_id_tag}[PROTHEUS] Pedido: {pedido_id} | {status_orig} {motivo_atualizacao}"
                novas_linhas.append([cnpj, data_obj, tipo_novo, resumo_txt, str(row['VENDEDOR']).upper().strip(), valor_novo])
            else: pulos += 1
            
        if novas_linhas:
            sheet.append_rows(novas_linhas)
            st.cache_data.clear() 
            return True, f"Processado: {contador_novos} novos."
        else: return True, "Tudo sincronizado."
    except Exception as e: return False, f"Erro: {e}"

# --- CALLBACKS DE SALVAMENTO ---
def processar_salvamento_lead(usuario_logado):
    nome, doc, val = st.session_state["novo_nome"], st.session_state["novo_doc"], st.session_state["novo_val"]
    if not nome or not doc: st.error("Erro: Nome e CPF/CNPJ obrigatórios.")
    else:
        if salvar_novo_lead_completo(doc, nome, st.session_state["novo_contato"], st.session_state["novo_tel"], usuario_logado, st.session_state["novo_origem"], st.session_state["novo_acao"], st.session_state["novo_resumo"], val):
            st.success("Lead Salvo!")
            st.session_state["novo_nome"] = ""
            st.session_state["novo_doc"] = ""
            st.session_state["novo_val"] = 0

def processar_salvamento_vendedor(cid, usuario_logado, tipo_selecionado):
    obs = st.session_state["obs_temp"]
    val = int(st.session_state.get("val_temp", 0)) if isinstance(st.session_state.get("val_temp", 0), (int, float)) else 0
    if salvar_interacao_nuvem(cid, datetime.now(), tipo_selecionado, obs, usuario_logado, val):
        st.success("Salvo! (Cache Local)")
        st.session_state["obs_temp"] = ""
        st.session_state["val_temp"] = 0

def fechar_proposta_automatica(cid, usuario_logado, proposta_row, status_novo):
    valor = proposta_row['Valor_Proposta']
    resumo = proposta_row['Resumo']
    obs = f"Ref. Proposta {extrair_id(resumo) or '(S/ ID)'}. Detalhes: {resumo}"
    if salvar_interacao_nuvem(cid, datetime.now(), status_novo, obs, usuario_logado, valor):
        st.success(f"{status_novo}!")
        time.sleep(0.5)
        st.rerun()

# --- APP PRINCIPAL ---
try:
    if URL_LOGO: st.sidebar.image(URL_LOGO, width=150)
    st.sidebar.title("CRM 22.1 Turbo")
    
    if st.sidebar.button("🔄 Atualizar Dados"):
        st.cache_data.clear()
        st.rerun()
    
    # 1. Carregamento Otimizado
    df_config, df_protheus = carregar_dados_estaticos()
    
    # 2. Carregamento Dinâmico
    if 'dados_interacoes' not in st.session_state:
        df_leads_raw, df_inter_raw = carregar_dados_dinamicos()
        st.session_state['dados_leads'] = df_leads_raw
        st.session_state['dados_interacoes'] = df_inter_raw
    
    df_leads = st.session_state['dados_leads']
    df_interacoes = st.session_state['dados_interacoes']

    # Merge de Clientes
    if not df_leads.empty:
        df_clientes = pd.concat([df_protheus, df_leads], ignore_index=True)
    else: df_clientes = df_protheus
    
    # --- CORREÇÃO DO ERRO CRÍTICO ---
    # Garantir que a coluna Nome_Cliente existe antes de mapear
    if not df_interacoes.empty:
        if 'Nome_Cliente' not in df_interacoes.columns:
            df_interacoes['Nome_Cliente'] = None

    # Mapeamento de Nomes
    if not df_clientes.empty and not df_interacoes.empty:
        mapa = dict(zip(df_clientes['ID_Cliente_CNPJ_CPF'].astype(str), df_clientes['Nome_Fantasia']))
        df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
        mask_nome = (df_interacoes['Nome_Cliente'].isna()) | (df_interacoes['Nome_Cliente'] == "Atualizando...")
        # FillNa garante que não dê erro se o cliente não for encontrado
        df_interacoes.loc[mask_nome, 'Nome_Cliente'] = df_interacoes.loc[mask_nome, 'CNPJ_Cliente'].map(mapa).fillna("Cliente Carteira")

    if not df_config.empty:
        # LOGIN
        usuarios_validos = sorted(df_config['Usuario'].unique().tolist())
        if 'logado' not in st.session_state: st.session_state['logado'] = False
        
        if not st.session_state['logado']:
            st.sidebar.markdown("### 🔒 Login")
            u = st.sidebar.selectbox("Usuário:", usuarios_validos)
            p = st.sidebar.text_input("Senha:", type="password")
            if st.sidebar.button("Entrar"):
                ud = df_config[df_config['Usuario'] == u].iloc[0]
                if str(ud['Senha']).strip() == str(p).strip():
                    st.session_state['logado'] = True; st.session_state['usuario_atual'] = u; st.rerun()
                else: st.sidebar.error("Senha inválida.")
            st.stop()
        
        usuario_logado = st.session_state['usuario_atual']
        if st.sidebar.button(f"Sair ({usuario_logado})"):
            st.session_state['logado'] = False; st.rerun()

        user_data = df_config[df_config['Usuario'] == usuario_logado].iloc[0]
        tipo_usuario = str(user_data['Tipo']).upper().strip()
        carteiras_permitidas = [x.strip() for x in str(user_data['Carteira_Alvo']).split(',')]

        # --- PROCESSAMENTO GERAL ---
        hoje = datetime.now().date()
        if 'Data_Ultima_Compra' in df_clientes.columns: df_clientes['Dias_Sem_Comprar'] = (pd.Timestamp(hoje) - df_clientes['Data_Ultima_Compra']).dt.days
        else: df_clientes['Dias_Sem_Comprar'] = 0

        # Prepara IDs Baixados
        ids_resolvidos = []
        pedidos_baixados = set()
        if not df_interacoes.empty:
            resolvidos = df_interacoes[df_interacoes['Tipo'].isin(['Venda Fechada', 'Venda Perdida'])]
            for t in resolvidos['Resumo'].astype(str):
                if i := extrair_id(t): ids_resolvidos.append(i)
                if p := extrair_pedido_protheus(t): pedidos_baixados.add(p)

        def calcular_status(linha):
            c = str(linha['ID_Cliente_CNPJ_CPF'])
            st_c = '🟢 ATIVO'
            if pd.isna(linha['Dias_Sem_Comprar']): st_c = '🆕 NOVO'
            elif linha['Dias_Sem_Comprar'] >= 60: st_c = '🔴 RECUPERAR'

            if not df_interacoes.empty:
                filt = df_interacoes[df_interacoes['CNPJ_Cliente'] == c]
                if not filt.empty:
                    aberto = False
                    for _, r in filt[filt['Tipo'] == 'Orçamento Enviado'].iterrows():
                        mid = extrair_id(r['Resumo'])
                        mped = extrair_pedido_protheus(r['Resumo'])
                        resolv = False
                        if mid and mid in ids_resolvidos: resolv = True
                        if mped and mped in pedidos_baixados: resolv = True
                        if not resolv: aberto = True
                    
                    if aberto: return '⏳ NEGOCIAÇÃO'
                    
                    ult = filt.sort_values('Data_Obj').iloc[-1]
                    try:
                        if pd.notna(ult['Data_Obj']):
                            if ult['Tipo'] == 'Venda Fechada': return '⭐ VENDA RECENTE'
                            elif ult['Tipo'] == 'Venda Perdida': return '👎 VENDA PERDIDA'
                            elif ult['Tipo'] in ['Ligação Realizada', 'WhatsApp Enviado']: return '💬 CONTATADO'
                    except: pass
            return st_c
        
        df_clientes['Status'] = df_clientes.apply(calcular_status, axis=1)

        # --- SIDEBAR: METAS ---
        prim_dia = hoje.replace(day=1)
        meta_fat = int(user_data.get('Meta_Fat', 0))
        meta_cli = int(user_data.get('Meta_Clientes', 0))
        meta_ativ = int(user_data.get('Meta_Atividades', 0))
        
        real_fat = 0; real_cli = 0; real_ativ = 0
        if not df_interacoes.empty:
            m_mes = (df_interacoes['Vendedor'] == usuario_logado) & (df_interacoes['Data_Obj'] >= prim_dia)
            df_mes = df_interacoes[m_mes]
            if not df_mes.empty:
                real_fat = int(df_mes[df_mes['Tipo'] == 'Venda Fechada']['Valor_Proposta'].sum())
                real_cli = df_mes[df_mes['Tipo'] == 'Venda Fechada']['CNPJ_Cliente'].nunique()
                real_ativ = len(df_mes[df_mes['Tipo'].isin(['Ligação Realizada', 'WhatsApp Enviado', 'Agendou Visita'])])

        st.sidebar.markdown("### 🎯 Performance")
        st.sidebar.caption(f"💰 Fat: {formatar_moeda_visual(real_fat)} / {formatar_moeda_visual(meta_fat)}")
        st.sidebar.progress(min(real_fat/meta_fat, 1.0) if meta_fat > 0 else 0)
        st.sidebar.caption(f"👥 Clientes: {real_cli} / {meta_cli}")
        st.sidebar.progress(min(real_cli/meta_cli, 1.0) if meta_cli > 0 else 0)
        st.sidebar.caption(f"🔨 Atividades: {real_ativ} / {meta_ativ}")
        st.sidebar.progress(min(real_ativ/meta_ativ, 1.0) if meta_ativ > 0 else 0)

        # --- SIDEBAR: AÇÕES ---
        if tipo_usuario == "GESTOR":
            with st.sidebar.expander("📥 Importar Protheus"):
                up = st.file_uploader("XLSX", type=["xlsx"])
                if up and st.button("Processar"):
                    suc, m = processar_arquivo_protheus(up, df_interacoes)
                    if suc: st.success(m)
                    else: st.error(m)

        if tipo_usuario == "VENDEDOR" or "TODOS" in carteiras_permitidas:
            with st.sidebar.expander("➕ Novo Lead"):
                # Form Lead
                for k in ["novo_nome", "novo_doc", "novo_contato", "novo_tel", "novo_resumo"]:
                    if k not in st.session_state: st.session_state[k] = ""
                if "novo_val" not in st.session_state: st.session_state["novo_val"] = 0
                if "novo_origem" not in st.session_state: st.session_state["novo_origem"] = "SELECIONE..."
                if "novo_acao" not in st.session_state: st.session_state["novo_acao"] = "SELECIONE..."
                
                st.text_input("Nome", key="novo_nome")
                st.text_input("CNPJ/CPF", key="novo_doc")
                st.text_input("Contato", key="novo_contato")
                st.text_input("Tel", key="novo_tel")
                st.selectbox("Origem", ["SELECIONE...", "LIGAÇÃO", "E-MAIL", "INDICAÇÃO"], key="novo_origem")
                st.selectbox("Ação", ["SELECIONE...", "Ligação Realizada", "Orçamento Enviado"], key="novo_acao")
                if st.session_state["novo_acao"] == "Orçamento Enviado":
                    st.number_input("Valor", step=1, key="novo_val")
                st.text_area("Resumo", key="novo_resumo")
                st.button("💾 Salvar", on_click=processar_salvamento_lead, args=(usuario_logado,))

        # --- FILTROS ---
        if "TODOS" in carteiras_permitidas:
            meus_clientes = df_clientes
            minhas_interacoes = df_interacoes
        else:
            meus_clientes = df_clientes[df_clientes['Ultimo_Vendedor'].isin(carteiras_permitidas)] if 'Ultimo_Vendedor' in df_clientes.columns else pd.DataFrame()
            minhas_interacoes = df_interacoes[df_interacoes['Vendedor'].isin(carteiras_permitidas)] if not df_interacoes.empty else pd.DataFrame()

        # --- VIEW GESTOR ---
        if tipo_usuario == "GESTOR":
            st.divider(); st.title("📊 Painel Geral")
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns(4)
                d_ini = c1.date_input("De", value=hoje-timedelta(days=30))
                d_fim = c2.date_input("Até", value=hoje)
                vends = c4.multiselect("Vendedores", minhas_interacoes['Vendedor'].unique() if not minhas_interacoes.empty else [])
            
            if not minhas_interacoes.empty:
                msk = (minhas_interacoes['Data_Obj'] >= d_ini) & (minhas_interacoes['Data_Obj'] <= d_fim)
                if vends: msk = msk & (minhas_interacoes['Vendedor'].isin(vends))
                df_f = minhas_interacoes[msk].copy()
                
                # KPIs
                orcado = int(df_f[df_f['Tipo'] == 'Orçamento Enviado']['Valor_Proposta'].sum())
                fechado = int(df_f[df_f['Tipo'] == 'Venda Fechada']['Valor_Proposta'].sum())
                
                # Na Mesa
                mesa = 0
                for _, r in df_f[df_f['Tipo'] == 'Orçamento Enviado'].iterrows():
                    pid = extrair_id(r['Resumo'])
                    ped = extrair_pedido_protheus(r['Resumo'])
                    if not ((pid and pid in ids_resolvidos) or (ped and ped in pedidos_baixados)):
                        mesa += r['Valor_Proposta']
                
                kc1, kc2, kc3, kc4 = st.columns(4)
                kc1.metric("💰 Orçado", formatar_moeda_visual(orcado))
                kc2.metric("🔮 Na Mesa", formatar_moeda_visual(mesa))
                kc3.metric("✅ Fechado", formatar_moeda_visual(fechado))
                kc4.metric("📞 Interações", len(df_f))

                t1, t2 = st.tabs(["🏆 Ranking", "📝 Detalhes"])
                with t1:
                    if not df_f.empty:
                        df_f['Is_Fechado'] = (df_f['Tipo'] == 'Venda Fechada').astype(int)
                        df_f['Val_Venda'] = df_f.apply(lambda x: x['Valor_Proposta'] if x['Tipo']=='Venda Fechada' else 0, axis=1)
                        rank = df_f.groupby('Vendedor').agg(Fat=('Val_Venda','sum'), Cli=('CNPJ_Cliente',lambda x: x[df_f.loc[x.index,'Tipo']=='Venda Fechada'].nunique())).reset_index()
                        
                        df_m = df_config[['Usuario','Meta_Fat','Meta_Clientes']].rename(columns={'Usuario':'Vendedor'})
                        rank = pd.merge(rank, df_m, on='Vendedor', how='left').fillna(0)
                        
                        rank['Fat R$'] = rank['Fat'].apply(formatar_moeda_visual)
                        rank['% Fat'] = rank.apply(lambda x: f"{x['Fat']/x['Meta_Fat']*100:.0f}%" if x['Meta_Fat']>0 else "-", axis=1)
                        rank['% Cli'] = rank.apply(lambda x: f"{x['Cli']/x['Meta_Clientes']*100:.0f}%" if x['Meta_Clientes']>0 else "-", axis=1)
                        st.dataframe(rank[['Vendedor','Fat R$','% Fat','Cli','% Cli']], use_container_width=True)
                with t2:
                    st.dataframe(df_f[['Data_Obj','Nome_Cliente','Tipo','Resumo','Valor_Proposta','Vendedor']], use_container_width=True)

        # --- VIEW VENDEDOR ---
        else:
            st.divider(); st.title("💼 Carteira")
            c_esq, c_dir = st.columns([1,1])
            with c_esq:
                tb = st.text_input("🔍 Buscar Cliente")
                if tb:
                    tb = tb.upper()
                    lst = meus_clientes[meus_clientes['Nome_Fantasia'].str.upper().str.contains(tb, na=False) | meus_clientes['ID_Cliente_CNPJ_CPF'].astype(str).str.contains(tb, na=False)]
                else:
                    st_sel = st.multiselect("Status", ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO'], default=['⏳ NEGOCIAÇÃO'])
                    lst = meus_clientes[meus_clientes['Status'].isin(st_sel)].sort_values('Status')
                
                cid = st.radio("Selecione", lst['ID_Cliente_CNPJ_CPF'].tolist(), format_func=lambda x: f"{formatar_documento(x)} | {lst[lst['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]}") if not lst.empty else None

            with c_dir:
                if cid and not meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF']==cid].empty:
                    cli = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF']==cid].iloc[0]
                    with st.container(border=True):
                        st.markdown(f"### {cli['Nome_Fantasia']}")
                        c1, c2 = st.columns(2)
                        c1.write(f"**👤** {cli.get('Contato','-')}")
                        c1.write(f"**📞** {cli.get('Telefone_Contato1','-')}")
                        c1.write(f"**📍** {cli.get('Cidade','-')}/{cli.get('UF','-')}")
                        c2.write(f"**👔** {cli.get('Ultimo_Vendedor','-')}")
                        c2.write(f"**💰** {formatar_moeda_visual(cli.get('Total_Compras',0))}")
                        c2.write(f"**📅** {formatar_data_br(cli.get('Data_Ultima_Compra','-'))}")
                        
                        st.divider()
                        th, tp, tn = st.tabs(["📜 Hist", "💰 Abertas", "📝 Ação"])
                        with th:
                            if not minhas_interacoes.empty:
                                hist = minhas_interacoes[minhas_interacoes['CNPJ_Cliente']==str(cid)].sort_values('Data_Obj', ascending=False)
                                if not hist.empty:
                                    # Fix visual bug: convert to string/formatted before display
                                    hist_d = hist[['Data_Obj','Tipo','Resumo','Valor_Proposta']].copy()
                                    hist_d['Valor_Proposta'] = hist_d['Valor_Proposta'].apply(formatar_moeda_visual)
                                    hist_d['Data_Obj'] = hist_d['Data_Obj'].apply(formatar_data_br)
                                    st.dataframe(hist_d.head(5), hide_index=True, use_container_width=True)
                        with tp:
                            if not minhas_interacoes.empty:
                                props = minhas_interacoes[(minhas_interacoes['CNPJ_Cliente']==str(cid)) & (minhas_interacoes['Tipo']=='Orçamento Enviado')].copy()
                                abertas = []
                                for _, r in props.sort_values('Data_Obj', ascending=False).iterrows():
                                    pid = extrair_id(r['Resumo'])
                                    ped = extrair_pedido_protheus(r['Resumo'])
                                    if not ((pid and pid in ids_resolvidos) or (ped and ped in pedidos_baixados)): abertas.append(r)
                                
                                for idx, row in enumerate(abertas):
                                    with st.container(border=True):
                                        ca, cb, cc = st.columns([2,1,1])
                                        ca.markdown(f"**{formatar_data_br(row['Data_Obj'])}** | {formatar_moeda_visual(row['Valor_Proposta'])}")
                                        ca.caption(row['Resumo'])
                                        if cb.button("✅", key=f"w_{idx}"): fechar_proposta_automatica(cid, usuario_logado, row, "Venda Fechada")
                                        if cc.button("❌", key=f"l_{idx}"): fechar_proposta_automatica(cid, usuario_logado, row, "Venda Perdida")
                        with tn:
                            tipo = st.selectbox("Tipo", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"])
                            if tipo == "Orçamento Enviado":
                                st.number_input("R$", step=1, key="val_temp")
                            else: st.session_state["val_temp"] = 0
                            st.text_area("Obs", key="obs_temp")
                            st.button("💾 Salvar Interação", type="primary", on_click=processar_salvamento_vendedor, args=(cid, usuario_logado, tipo))

except Exception as e:
    st.error(f"Erro Crítico: {e}")
